package xclient

import (
	"log"
	"net/http"
	"strings"
	"time"
)

type AplRegistryDiscovery struct {
	*MultiServersDiscovery               //继承MultiServersDiscovery,复用其能力
	registry               string        //注册中心地址（http地址）
	timeout                time.Duration //服务列表的过期时间
	lastUpdate             time.Time     //记录上次从注册中心更新的时间
}

const defaultUpdateTimeout = time.Second * 10 //默认10秒过期

func NewAplRegistryDiscovery(registerAddr string, timeout time.Duration) *AplRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	d := &AplRegistryDiscovery{
		MultiServersDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry:              registerAddr,
		timeout:               timeout,
	}
	return d
}

func (d *AplRegistryDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	d.lastUpdate = time.Now()
	return nil
}

func (d *AplRegistryDiscovery) Refresh() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)
	resp, err := http.Get(d.registry)
	if err != nil {
		log.Println("rpc regitry refresh err:", err)
		return err
	}
	servers := strings.Split(resp.Header.Get("X-Aplrpc-Servers"), ",")
	d.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			d.servers = append(d.servers, strings.TrimSpace(server))
		}
	}
	d.lastUpdate = time.Now()
	return nil
}

func (d *AplRegistryDiscovery) Get(mode SelectMode) (string, error) {
	if err := d.Refresh(); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.Get(mode)
}

func (d *AplRegistryDiscovery) GetAll() ([]string, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll()
}
