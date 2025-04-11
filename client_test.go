package aplrpc

import (
	"context"
	"net"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

type Bar int

func (b Bar) Timeout(argv int, reply *int) error {
	time.Sleep(time.Second * 2)
	return nil
}

func startServer(addr chan string) {
	var b Bar
	_ = Register(&b)
	l, _ := net.Listen("tcp", ":0")
	addr <- l.Addr().String()
	Accept(l)
}

// 第一个测试用例，用于测试连接超时。NewClient 函数耗时 2s:
// ConnectionTimeout 分别设置为 1s 和 0 两种场景。
func TestClient_dialTimeout(t *testing.T) {
	t.Parallel()
	l, _ := net.Listen("tcp", ":0")
	f := func(conn net.Conn, opt *Option) (client *Client, err error) {
		_ = conn.Close()
		time.Sleep(time.Second * 2)
		return nil, nil
	}
	t.Run("timeout", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", l.Addr().String(), &Option{ConnectTimeout: time.Second})
		_assert(err != nil && strings.Contains(err.Error(), "connect timeout"), "expect a timeout error")
	})
	t.Run("0", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", l.Addr().String(), &Option{ConnectTimeout: 0})
		_assert(err == nil, "0 means no limit")
	})
}

func TestClient_Call(t *testing.T) {
	t.Parallel()
	addrCh := make(chan string)
	go startServer(addrCh)
	addr := <-addrCh
	time.Sleep(time.Second)
	t.Run("client timeout", func(t *testing.T) {
		client, _ := Dial("tcp", addr)
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		var reply int
		err := client.Call(ctx, "Bar.Timeout", 1, &reply)
		_assert(err != nil && strings.Contains(err.Error(), ctx.Err().Error()), "expect a timeout error")
	})
	t.Run("server handle timeout", func(t *testing.T) {
		client, _ := Dial("tcp", addr, &Option{
			HandleTimeout: time.Second,
		})
		var reply int
		err := client.Call(context.Background(), "Bar.Timeout", 1, &reply)
		_assert(err != nil && strings.Contains(err.Error(), "handle timeout"), "expect  a timeout error")
	})
}

func TestSDial(t *testing.T) {
	ch := make(chan struct{})
	var network, addr string

	if runtime.GOOS == "linux" {
		network = "unix"
		addr = "/tmp/aplrpc.sock"
	} else {
		network = "tcp"
		addr = "127.0.0.1:9999"
	}

	go func() {
		if network == "unix" {
			_ = os.Remove(addr)
		}
		l, err := net.Listen(network, addr)
		if err != nil {
			t.Fatalf("failed to listen %s socket: %v", network, err)
		}
		ch <- struct{}{}
		Accept(l) // 这里是服务器监听
	}()
	<-ch

	client, err := XDial(network + "@" + addr)
	if err != nil {
		t.Fatalf("failed to connect to %s socket: %v", network, err)
	}
	_assert(client != nil, "Client is nil, please check client initialization") // 确保 client 不为 nil
}

////下面这段代码无法在windows系统上运行：
//func TestSDial(t *testing.T) {
//	if runtime.GOOS == "linux" {
//		ch := make(chan struct{})
//		addr := "/tmp/aplrpc.sock"
//		go func() {
//			_ = os.Remove(addr)
//			l, err := net.Listen("unix", addr)
//			if err != nil {
//				t.Fatal("failed to listen unix socket")
//			}
//			ch <- struct{}{}
//			Accept(l)
//		}()
//		<-ch
//		_, err := XDial("unix@" + addr)
//		_assert(err == nil, "failed to connect unix socket")
//	}
//}
