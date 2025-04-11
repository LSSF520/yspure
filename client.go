package aplrpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aplrpc/Codec/codec"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Call struct {
	Seq           uint64      //序列号，唯一标识一次RPC调用
	ServiceMethod string      //RPC方法名称，格式："<service>.<method>"
	Args          interface{} //方法的参数
	Reply         interface{} //方法的返回值（指针）
	Error         error       //RPC的调用可能发生的错误
	Done          chan *Call  //该RPC调用完成时，向Done发送信号
}

// done方法：向Done通道发送自身call,表明RPC调用完成
// RPC客户端可以通过监听call.Done,在RPC结束时异步获取结果
func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc       codec.Codec      //客户端的编解码器，对请求/响应进行序列化和反序列化
	opt      *Option          //配置选项，例如超时、压缩等
	sending  sync.Mutex       //锁，保证同一时刻只有一个请求被发送，防止数据错乱
	header   codec.Header     //RPC请求的消息头
	mu       sync.Mutex       //保证seq以及pending等字段的线程安全
	seq      uint64           //递增的请求编号，保证每个请求的唯一性
	pending  map[uint64]*Call //记录正在等待响应的请求，键是seq，值是call结构体
	closing  bool             //用户手动关闭客户端
	shutdown bool             //服务器通知客户端关闭（如连接异常）
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// 关闭连接：
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close() //关闭底层网络连接
}

// 检查客户端是否可用：
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// call方法：
// 注册请求：
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown //如果客户端已关闭就返回错误信息
	}
	call.Seq = client.seq           //为该call赋予唯一的seq号
	client.pending[call.Seq] = call //存入pending(等待响应的请求)
	client.seq++                    //自增seq,保证唯一性
	return call.Seq, nil
}

// 删除请求：
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq] //根据传入的seq找到对应的call
	delete(client.pending, seq) //并从该pending(map)中移除该call
	return call                 //如果该请求存在会返回*Call，调用者可以获取Reply和Error。当不存在时（可能已完成或者超时）返回nil
}

// 终止所有请求：
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true                //设置客户端关闭，防止新的请求注册
	for _, call := range client.pending { //遍历pending，将所有未完成的call标记为错误，并触发done()
		call.Error = err //设置错误信息
		call.done()      //通知等待该请求的goroutine
	}
}

// 发送信息
func (client *Client) send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()

	//注册
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}
	//赋值
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""
	//管道写入
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// 接收功能：
func (client *Client) receive() {
	var err error
	for err == nil { //循环遍历所有请求，直到出错
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq) //删除它在pending中的信息，并返回该信息
		switch {
		case call == nil: //为空，返回错误信息
			err = client.cc.ReadBody(nil)
		case h.Error != "": //不为空，但是服务端处理出错
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default: //正确：服务端处理正常，需要从body中读取reply的值
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body" + err.Error())
			}
			call.done()
		}
	}
	//即使有未处理完的请求也全部进行终止
	client.terminateCalls(err)
}

// 异步接口
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client:done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// 同步接口（对Go的封装）
func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
}

// 解析options选项
func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("numbers of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// 创建client实例：
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	//log.Println("Connecting to RPC server...") //Debug:连接开始（新加）
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client:codec error:", err)
		return nil, err
	}
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client:options error:", err)
		_ = conn.Close()
		return nil, err
	}
	//下面为新加的：
	//client := newClientCodec(f(conn), opt)
	//log.Println("Connected to RPC server successfully!")
	//return client, nil
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1, //从1开始，0无效
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive() //开启一个子协程 receive()，用于接收服务器的响应。
	return client
}

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		//log.Println("rpc client: parse options error:", err) //新加
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		//log.Println("rpc client: dial error:", err) //新加
		return nil, err
	}
	defer func() {
		if err != nil {
			//log.Println("rpc client: connection failed, closing connection:", err) //新加
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult)
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	select {
	case <-time.After(opt.ConnectTimeout):
		//log.Println("rpc client: connect timeout") //新加
		return nil, fmt.Errorf("rpc client:connect timeout:expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

func Dial(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}

func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))

	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	//下面为原码，以上为我的修改：
	if err == nil && resp.Status == connected {
		return NewClient(conn, opt)
	}
	if err == nil { //改了一下
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		return Dial(protocol, addr, opts...)
	}
}

//Copyright 2009 The Go Authors. All rights reserved.
//Use of this source code is governed by a BSD-style
//license that can be found in the LICENSE file.

//package aplrpc
//
//import (
//	"bufio"
//	"context"
//	"encoding/json"
//	"errors"
//	"fmt"
//	"github.com/aplrpc/Codec/codec"
//	"io"
//	"log"
//	"net"
//	"net/http"
//	"strings"
//	"sync"
//	"time"
//)
//
//// Call represents an active RPC.
//type Call struct {
//	Seq           uint64
//	ServiceMethod string      // format "<service>.<method>"
//	Args          interface{} // arguments to the function
//	Reply         interface{} // reply from the function
//	Error         error       // if error occurs, it will be set
//	Done          chan *Call  // Strobes when call is complete.
//}
//
//func (call *Call) done() {
//	call.Done <- call
//}
//
//// Client represents an RPC Client.
//// There may be multiple outstanding Calls associated
//// with a single Client, and a Client may be used by
//// multiple goroutines simultaneously.
//type Client struct {
//	cc       codec.Codec
//	opt      *Option
//	sending  sync.Mutex // protect following
//	header   codec.Header
//	mu       sync.Mutex // protect following
//	seq      uint64
//	pending  map[uint64]*Call
//	closing  bool // user has called Close
//	shutdown bool // server has told us to stop
//}
//
//var _ io.Closer = (*Client)(nil)
//
//var ErrShutdown = errors.New("connection is shut down")
//
//// Close the connection
//func (client *Client) Close() error {
//	client.mu.Lock()
//	defer client.mu.Unlock()
//	if client.closing {
//		return ErrShutdown
//	}
//	client.closing = true
//	return client.cc.Close()
//}
//
//// IsAvailable return true if the client does work
//func (client *Client) IsAvailable() bool {
//	client.mu.Lock()
//	defer client.mu.Unlock()
//	return !client.shutdown && !client.closing
//}
//
//func (client *Client) registerCall(call *Call) (uint64, error) {
//	client.mu.Lock()
//	defer client.mu.Unlock()
//	if client.closing || client.shutdown {
//		return 0, ErrShutdown
//	}
//	call.Seq = client.seq
//	client.pending[call.Seq] = call
//	client.seq++
//	return call.Seq, nil
//}
//
//func (client *Client) removeCall(seq uint64) *Call {
//	client.mu.Lock()
//	defer client.mu.Unlock()
//	call := client.pending[seq]
//	delete(client.pending, seq)
//	return call
//}
//
//func (client *Client) terminateCalls(err error) {
//	client.sending.Lock()
//	defer client.sending.Unlock()
//	client.mu.Lock()
//	defer client.mu.Unlock()
//	client.shutdown = true
//	for _, call := range client.pending {
//		call.Error = err
//		call.done()
//	}
//}
//
//func (client *Client) send(call *Call) {
//	// make sure that the client will send a complete request
//	client.sending.Lock()
//	defer client.sending.Unlock()
//
//	// register this call.
//	seq, err := client.registerCall(call)
//	if err != nil {
//		call.Error = err
//		call.done()
//		return
//	}
//
//	// prepare request header
//	client.header.ServiceMethod = call.ServiceMethod
//	client.header.Seq = seq
//	client.header.Error = ""
//
//	// encode and send the request
//	if err := client.cc.Write(&client.header, call.Args); err != nil {
//		call := client.removeCall(seq)
//		// call may be nil, it usually means that Write partially failed,
//		// client has received the response and handled
//		if call != nil {
//			call.Error = err
//			call.done()
//		}
//	}
//}
//
//func (client *Client) receive() {
//	var err error
//	for err == nil {
//		var h codec.Header
//		if err = client.cc.ReadHeader(&h); err != nil {
//			break
//		}
//		call := client.removeCall(h.Seq)
//		switch {
//		case call == nil:
//			// it usually means that Write partially failed
//			// and call was already removed.
//			err = client.cc.ReadBody(nil)
//		case h.Error != "":
//			call.Error = fmt.Errorf(h.Error)
//			err = client.cc.ReadBody(nil)
//			call.done()
//		default:
//			err = client.cc.ReadBody(call.Reply)
//			if err != nil {
//				call.Error = errors.New("reading body " + err.Error())
//			}
//			call.done()
//		}
//	}
//	// error occurs, so terminateCalls pending calls
//	client.terminateCalls(err)
//}
//
//// Go invokes the function asynchronously.
//// It returns the Call structure representing the invocation.
//func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
//	if done == nil {
//		done = make(chan *Call, 10)
//	} else if cap(done) == 0 {
//		log.Panic("rpc client: done channel is unbuffered")
//	}
//	call := &Call{
//		ServiceMethod: serviceMethod,
//		Args:          args,
//		Reply:         reply,
//		Done:          done,
//	}
//	client.send(call)
//	return call
//}
//
//// Call invokes the named function, waits for it to complete,
//// and returns its error status.
//func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
//	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
//	select {
//	case <-ctx.Done():
//		client.removeCall(call.Seq)
//		return errors.New("rpc client: call failed: " + ctx.Err().Error())
//	case call := <-call.Done:
//		return call.Error
//	}
//}
//
//func parseOptions(opts ...*Option) (*Option, error) {
//	// if opts is nil or pass nil as parameter
//	if len(opts) == 0 || opts[0] == nil {
//		return DefaultOption, nil
//	}
//	if len(opts) != 1 {
//		return nil, errors.New("number of options is more than 1")
//	}
//	opt := opts[0]
//	opt.MagicNumber = DefaultOption.MagicNumber
//	if opt.CodecType == "" {
//		opt.CodecType = DefaultOption.CodecType
//	}
//	return opt, nil
//}
//
//func NewClient(conn net.Conn, opt *Option) (*Client, error) {
//	f := codec.NewCodecFuncMap[opt.CodecType]
//	if f == nil {
//		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
//		log.Println("rpc client: codec error:", err)
//		return nil, err
//	}
//	// send options with server
//	if err := json.NewEncoder(conn).Encode(opt); err != nil {
//		log.Println("rpc client: options error: ", err)
//		_ = conn.Close()
//		return nil, err
//	}
//	return newClientCodec(f(conn), opt), nil
//}
//
//func newClientCodec(cc codec.Codec, opt *Option) *Client {
//	client := &Client{
//		seq:     1, // seq starts with 1, 0 means invalid call
//		cc:      cc,
//		opt:     opt,
//		pending: make(map[uint64]*Call),
//	}
//	go client.receive()
//	return client
//}
//
//type clientResult struct {
//	client *Client
//	err    error
//}
//
//type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)
//
//func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
//	opt, err := parseOptions(opts...)
//	if err != nil {
//		return nil, err
//	}
//	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
//	if err != nil {
//		return nil, err
//	}
//	// close the connection if client is nil
//	defer func() {
//		if err != nil {
//			_ = conn.Close()
//		}
//	}()
//	ch := make(chan clientResult)
//	go func() {
//		client, err := f(conn, opt)
//		ch <- clientResult{client: client, err: err}
//	}()
//	if opt.ConnectTimeout == 0 {
//		result := <-ch
//		return result.client, result.err
//	}
//	select {
//	case <-time.After(opt.ConnectTimeout):
//		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
//	case result := <-ch:
//		return result.client, result.err
//	}
//}
//
//// Dial connects to an RPC server at the specified network address
//func Dial(network, address string, opts ...*Option) (*Client, error) {
//	return dialTimeout(NewClient, network, address, opts...)
//}
//
//// NewHTTPClient new a Client instance via HTTP as transport protocol
//func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
//	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))
//
//	// Require successful HTTP response
//	// before switching to RPC protocol.
//
//	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
//	if err == nil && resp.Status == connected {
//		return NewClient(conn, opt)
//	}
//	if err == nil {
//		err = errors.New("unexpected HTTP response: " + resp.Status)
//	}
//	return nil, err
//}
//
//// DialHTTP connects to an HTTP RPC server at the specified network address
//// listening on the default HTTP RPC path.
//func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
//	return dialTimeout(NewHTTPClient, network, address, opts...)
//}
//
//// XDial calls different functions to connect to a RPC server
//// according the first parameter rpcAddr.
//// rpcAddr is a general format (protocol@addr) to represent a rpc server
//// eg, http@10.0.0.1:7001, tcp@10.0.0.1:9999, unix@/tmp/geerpc.sock
//func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
//	parts := strings.Split(rpcAddr, "@")
//	if len(parts) != 2 {
//		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
//	}
//	protocol, addr := parts[0], parts[1]
//	switch protocol {
//	case "http":
//		return DialHTTP("tcp", addr, opts...)
//	default:
//		// tcp, unix or other transport protocol
//		return Dial(protocol, addr, opts...)
//	}
//}
