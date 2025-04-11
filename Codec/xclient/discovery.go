package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	RandomSelect     SelectMode = iota //随机选择
	RoundRobinSelect                   //轮询选择
)

// 通用接口，适用于不同的服务发现实现，例如静态列表或从注册中心动态获取
type Discovery interface {
	Refresh() error                      //从远程注册中心刷新服务列表
	Update(servers []string) error       //更新服务列表
	Get(mode SelectMode) (string, error) //按策略获取一个服务
	GetAll() ([]string, error)           //获取所有服务实例
}

// MultiServersDiscovery 是一个不依赖注册中心的服务发现机制，
// 由用户手动提供服务器地址列表
type MultiServersDiscovery struct {
	r       *rand.Rand   //随机数生成器，随机负载均衡
	mu      sync.RWMutex //读写锁进行并发安全控制
	servers []string     //维护的服务器列表
	index   int          //记录轮询到的位置，用于Round Robin算法
}

func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		servers: servers,
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	d.index = d.r.Intn(math.MaxInt32 - 1)
	return d
}

var _Discovery = (*MultiServersDiscovery)(nil)

// Refresh 对于 MultiServersDiscovery 无意义，因为服务器列表是手动维护的
func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

func (d *MultiServersDiscovery) Update(servers []string) error {
	d.mu.Lock() //确保写入安全
	defer d.mu.Unlock()
	d.servers = servers
	return nil
}

// 根据负载均衡策略随机选择一个服务器
func (d *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no availiable servers")
	}
	switch mode {
	case RandomSelect:
		return d.servers[d.r.Intn(n)], nil
	case RoundRobinSelect:
		s := d.servers[d.index%n]
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

// 返回所有可用的服务器
func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	d.mu.RLock() //确保读取安全，防止并发读取冲突
	defer d.mu.RUnlock()
	servers := make([]string, len(d.servers), len(d.servers))
	copy(servers, d.servers) //返回servers 的拷贝，避免外部修改d.servers,导致数据不一致
	return servers, nil
}
