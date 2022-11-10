package discover

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
)

type watchCallBack func(op string, key string, value []byte, preValue []byte)

type StorageConfig struct {
	DialTimeout time.Duration
	Endpoints   []string
	Prefix      string
	LeaseTTL    time.Duration
	UserName    string
	Password    string
}

type Storage interface {
	Init() error
	Shutdown() error
	DelKey(key string, withPrefix bool) error
	SetString(key string, value string) error
	GetString(key string) (string, error)
	GetAllString(prefix string) ([]string, []string, error)
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	GetAll(prefix string) ([]string, [][]byte, error)
	Watch(prefix string, callback ...watchCallBack)
	Count(key string, withPrefix bool) (int64, error)
}

type EtcdKvStorage struct {
	cli             *clientv3.Client
	etcdEndpoints   []string
	etcdPrefix      string
	etcdDialTimeout time.Duration
	leaseTTL        time.Duration
	leaseID         clientv3.LeaseID
	stopChan        chan struct{}
	stopLeaseChan   chan bool
	callbacks       map[string][]watchCallBack
	etcdUser        string
	etcdPasswd      string
	revokeTimeout   time.Duration
	shutdownDelay   time.Duration
}

func NewEtcdKvStorage(conf StorageConfig) *EtcdKvStorage {
	return &EtcdKvStorage{
		etcdEndpoints:   conf.Endpoints,
		etcdPrefix:      conf.Prefix,
		etcdDialTimeout: conf.DialTimeout,
		leaseTTL:        conf.LeaseTTL,
		stopChan:        make(chan struct{}),
		stopLeaseChan:   make(chan bool),
		callbacks:       make(map[string][]watchCallBack),
		etcdUser:        conf.UserName,
		etcdPasswd:      conf.Password,
		revokeTimeout:   5 * time.Second,
		shutdownDelay:   300 * time.Millisecond,
	}
}

func (e *EtcdKvStorage) Init() error {
	var cli *clientv3.Client
	var err error
	if e.cli == nil {
		cli, err = clientv3.New(clientv3.Config{
			Endpoints:   e.etcdEndpoints,
			DialTimeout: e.etcdDialTimeout,
			Username:    e.etcdUser,
			Password:    e.etcdPasswd,
		})
		if err != nil {
			err = fmt.Errorf("[kv storage] init client error, %s", err)
			return err
		}
		e.cli = cli
	}
	// namespaced etcd :)
	e.cli.KV = namespace.NewKV(e.cli.KV, e.etcdPrefix)
	e.cli.Watcher = namespace.NewWatcher(e.cli.Watcher, e.etcdPrefix)
	if e.leaseTTL > 0 {
		fmt.Printf("etcd init,endpoints:%s user:%s password:%s \n", e.etcdEndpoints, e.etcdUser, e.etcdPasswd)
		e.cli.Lease = namespace.NewLease(e.cli.Lease, e.etcdPrefix)
		err = e.bootstrapLease()
		if err != nil {
			err = fmt.Errorf("[kv storage] bootstrapLease error, %s", err)
			return err
		}
	}
	return nil
}

// revoke prevents Pitaya from crashing when etcd is not available
func (e *EtcdKvStorage) revoke() error {
	close(e.stopLeaseChan)
	c := make(chan error)
	defer close(c)
	go func() {
		log.Println("waiting for etcd revoke")
		_, err := e.cli.Revoke(context.TODO(), e.leaseID)
		c <- err
		log.Println("finished waiting for etcd revoke")
	}()
	select {
	case err := <-c:
		return err // completed normally
	case <-time.After(e.revokeTimeout):
		log.Println("timed out waiting for etcd revoke")
		return nil // timed out
	}
}

func (e *EtcdKvStorage) Shutdown() error {
	// before
	e.revoke()
	time.Sleep(e.shutdownDelay) // Sleep for a short while to ensure shutdown has propagated
	// close
	close(e.stopChan)
	e.cli.Close()
	return nil
}

func (e *EtcdKvStorage) bootstrapLease() error {
	// grab lease
	l, err := e.cli.Grant(context.TODO(), int64(e.leaseTTL.Seconds()))
	if err != nil {
		return err
	}
	e.leaseID = l.ID
	log.Printf("[kv storage] got leaseID %x \n", l.ID)
	// this will keep alive forever, when channel c is closed
	// it means we probably have to rebootstrap the lease
	c, err := e.cli.KeepAlive(context.TODO(), e.leaseID)
	if err != nil {
		return err
	}
	// need to receive here as per etcd docs
	<-c
	go e.watchLeaseChan(c)
	return nil
}

func (e *EtcdKvStorage) watchLeaseChan(c <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		select {
		case <-e.stopChan:
			return
		case <-e.stopLeaseChan:
			return
		case kaRes := <-c:
			if kaRes == nil {
				log.Println("[kv storage] error renewing etcd lease, rebootstrapping")
				for {
					err := e.bootstrapLease()
					if err != nil {
						log.Println("[kv storage] error rebootstrapping lease, will retry in 5 seconds")
						time.Sleep(5 * time.Second)
						continue
					} else {
						return
					}
				}
			}
		}
	}
}

func (e *EtcdKvStorage) DelKey(key string, withPrefix bool) error {
	var err error
	ctxT := context.Background()
	if withPrefix {
		_, err = e.cli.Txn(ctxT).
			If(clientv3.Compare(clientv3.CreateRevision(key).WithPrefix(), ">", 0)).
			Then(clientv3.OpDelete(key, clientv3.WithPrefix())).
			Commit()
	} else {
		_, err = e.cli.Txn(ctxT).
			If(clientv3.Compare(clientv3.CreateRevision(key), ">", 0)).
			Then(clientv3.OpDelete(key)).
			Commit()
	}
	if err != nil {
		return err
	}

	return nil
}

func (e *EtcdKvStorage) SetString(key string, value string) error {
	if e.leaseTTL > 0 {
		_, err := e.cli.Put(context.Background(), key, value, clientv3.WithLease(e.leaseID))
		return err
	} else {
		_, err := e.cli.Put(context.Background(), key, value)
		return err
	}
}

func (e *EtcdKvStorage) GetString(key string) (string, error) {
	etcdRes, err := e.cli.Get(context.Background(), key)
	if err != nil {
		return "", err
	}
	if len(etcdRes.Kvs) == 0 {
		return "", fmt.Errorf("key not found in storage")
	}
	return string(etcdRes.Kvs[0].Value), nil
}

func (e *EtcdKvStorage) GetAllString(prefix string) ([]string, []string, error) {
	etcdRes, err := e.cli.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}
	keys := make([]string, len(etcdRes.Kvs))
	values := make([]string, len(etcdRes.Kvs))
	for i, kv := range etcdRes.Kvs {
		keys[i] = string(kv.Key)[len(prefix):]
		values[i] = string(kv.Value)
	}
	return keys, values, nil
}

func (e *EtcdKvStorage) Set(key string, value []byte) error {
	if e.leaseTTL > 0 {
		_, err := e.cli.Put(context.Background(), key, string(value), clientv3.WithLease(e.leaseID))
		return err
	} else {
		_, err := e.cli.Put(context.Background(), key, string(value))
		return err
	}
}

func (e *EtcdKvStorage) Get(key string) ([]byte, error) {
	etcdRes, err := e.cli.Get(context.Background(), key)
	if err != nil {
		return nil, err
	}
	if len(etcdRes.Kvs) == 0 {
		return nil, fmt.Errorf("key not found in storage")
	}
	return etcdRes.Kvs[0].Value, nil
}

func (e *EtcdKvStorage) GetAll(prefix string) ([]string, [][]byte, error) {
	etcdRes, err := e.cli.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}
	keys := make([]string, len(etcdRes.Kvs))
	values := make([][]byte, len(etcdRes.Kvs))
	for i, kv := range etcdRes.Kvs {
		keys[i] = string(kv.Key)[len(prefix):]
		values[i] = kv.Value
	}
	return keys, values, nil
}

func (e *EtcdKvStorage) watchEtcdChanges(prefix string) {
	w := e.cli.Watch(context.Background(), prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	failedWatchAttempts := 0
	go func(chn clientv3.WatchChan) {
		for {
			select {
			case wResp, ok := <-chn:
				if wResp.Err() != nil {
					log.Printf("[kv storage] etcd watcher response error: %s \n", wResp.Err())
					time.Sleep(100 * time.Millisecond)
				}
				if !ok {
					log.Println("[kv storage] etcd watcher died, retrying to watch in 1 second")
					failedWatchAttempts++
					time.Sleep(1000 * time.Millisecond)
					if failedWatchAttempts > 10 {
						if err := e.Init(); err != nil {
							failedWatchAttempts = 0
							continue
						}
						chn = e.cli.Watch(context.Background(), prefix, clientv3.WithPrefix())
						failedWatchAttempts = 0
					}
					continue
				}
				failedWatchAttempts = 0
				for _, ev := range wResp.Events {
					key := string(ev.Kv.Key)
					value := ev.Kv.Value
					var preValue []byte
					if ev.PrevKv != nil {
						preValue = ev.PrevKv.Value
					}
					switch ev.Type {
					case clientv3.EventTypePut:
						cbs := e.callbacks[prefix]
						for _, cb := range cbs {
							cb("put", key, value, preValue)
						}
					case clientv3.EventTypeDelete:
						cbs := e.callbacks[prefix]
						for _, cb := range cbs {
							cb("delete", key, value, preValue)
						}
					}
				}
			case <-e.stopChan:
				return
			}
		}

	}(w)
}

func (e *EtcdKvStorage) Watch(prefix string, callback ...watchCallBack) {
	if _, ok := e.callbacks[prefix]; !ok {
		cbs := make([]watchCallBack, 0)
		cbs = append(cbs, callback...)
		e.callbacks[prefix] = cbs
		e.watchEtcdChanges(prefix)
	} else {
		cbs := e.callbacks[prefix]
		cbs = append(cbs, callback...)
		e.callbacks[prefix] = cbs
	}
}

func (e *EtcdKvStorage) Count(key string, withPrefix bool) (int64, error) {
	var (
		etcdRes *clientv3.GetResponse
		err     error
	)
	if withPrefix {
		etcdRes, err = e.cli.Get(context.Background(), key, clientv3.WithPrefix(), clientv3.WithCountOnly())
	} else {
		etcdRes, err = e.cli.Get(context.Background(), key, clientv3.WithCountOnly())
	}
	if err != nil {
		return 0, err
	}
	return etcdRes.Count, nil
}
