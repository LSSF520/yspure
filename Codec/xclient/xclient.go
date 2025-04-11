package xclient

import (
	"context"
	. "github.com/aplrpc" //不在当前包中，需要用.导入相当于直接在包中定义,就可以直接导入。如果换成无.的形式那么只能用aplrpc.opion来使用否则会报错
	"io"
	"reflect"
	"sync"
)

type XClient struct {
	d       Discovery          //服务发现接口
	mode    SelectMode         //负载均衡模式
	opt     *Option            //协议选项
	mu      sync.Mutex         //互斥锁，保护clients映射的并发安全
	clients map[string]*Client //维护已建立的rpc连接，避免重复创建
}

var _ io.Closer = (*XClient)(nil)

func NewXClient(d Discovery, mode SelectMode, opt *Option) *XClient {
	return &XClient{d: d,
		mode:    mode,
		opt:     opt,
		clients: make(map[string]*Client), //初始化client缓存
	}
}

func (xc *XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for key, client := range xc.clients {
		_ = client.Close()      //关闭连接
		delete(xc.clients, key) //从clients缓存中删除
	}
	return nil
}

func (xc *XClient) dial(rpcAddr string) (*Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	client, ok := xc.clients[rpcAddr]
	if ok && !client.IsAvailable() { //如果缓存的Client失效，则删除
		_ = client.Close()
		delete(xc.clients, rpcAddr)
		client = nil
	}
	if client == nil { //没有缓存或缓存失效，则创建新的Cliennt
		var err error
		client, err = XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}
		xc.clients[rpcAddr] = client //存入缓存
	}
	return client, nil
}

func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	client, err := xc.dial(rpcAddr) //获取可用的客户端连接
	if err != nil {
		return err
	}
	return client.Call(ctx, serviceMethod, args, reply) //发送RPC请求
}

func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddr, err := xc.d.Get(xc.mode) //根据负载均衡策略选择一个服务器
	if err != nil {
		return err
	}
	return xc.call(rpcAddr, ctx, serviceMethod, args, reply) //调用远程方法
}

func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var e error
	replyDone := reply == nil
	ctx, cancel := context.WithCancel(ctx)
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			var clonedReply interface{}
			if reply != nil {
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := xc.call(rpcAddr, ctx, serviceMethod, args, clonedReply)
			mu.Lock()
			if err != nil && e == nil {
				e = err
				cancel()
			}
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return e
}
