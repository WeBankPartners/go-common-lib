package discover

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

type RegisterParam struct {
	ServerType    string    `json:"serverType"`    // 必传
	EtcdPrefix    string    `json:"etcdPrefix"`    // 必传
	EtcdEndpoints string    `json:"etcdEndpoints"` // 必传
	EtcdUser      string    `json:"etcdUser"`      // 启用了认证时传
	EtcdPassword  string    `json:"etcdPassword"`  // 启用了认证时传
	EtcdAuthKey   string    `json:"etcdAuthKey"`   // 该字段是base64(user,password)后的值，传了该字段可不传上面的user和password
	Metadata      *Metadata `json:"metadata"`      // 必传，暂时只支持http
}

type Metadata struct {
	HttpHost string `json:"httpHost,omitempty"`
	HttpPort string `json:"httpPort,omitempty"`
	TcpHost  string `json:"tcpHost,omitempty"`
	TcpPort  string `json:"tcpPort,omitempty"`
	GrpcHost string `json:"grpcHost,omitempty"`
	GrpcPort string `json:"grpcPort,omitempty"`
}

type EtcdServersData struct {
	Id       string    `json:"id"`
	Type     string    `json:"type"`
	Metadata *Metadata `json:"metadata"`
	Hostname string    `json:"hostname,omitempty"`
	CpuNum   int       `json:"cpuNum,omitempty"`
	CreateAt string    `json:"createAt"`
}

type DiscoveryServer struct {
	self            *EtcdServersData
	etcdStorage     *EtcdKvStorage
	serversMap      *sync.Map // map[serverType] -> []*EtcdServersData
	certsPrivateMap *sync.Map // map[privateKeyName] -> []byte
	certsPublicMap  *sync.Map // map[serverType] -> map[publicKeyName] -> []byte
}

func RegisterServer(param *RegisterParam) (ds *DiscoveryServer, err error) {
	if param.ServerType == "" || param.EtcdPrefix == "" || param.EtcdEndpoints == "" || param.Metadata == nil {
		err = fmt.Errorf("register param can not empty")
		return
	}
	// decode user password
	if param.EtcdAuthKey != "" {
		if authKeyBytes, decodeErr := base64.StdEncoding.DecodeString(param.EtcdAuthKey); decodeErr != nil {
			err = fmt.Errorf("base64 decode etcd auth key fail,%s ", decodeErr.Error())
			return
		} else {
			keySplit := strings.Split(string(authKeyBytes), ",")
			if len(keySplit) != 2 {
				err = fmt.Errorf("param etcd auth key illegal,should like base64(user,pass)")
				return
			}
			param.EtcdUser = keySplit[0]
			param.EtcdPassword = keySplit[1]
		}
	}
	// connect to etcd
	newDs := DiscoveryServer{serversMap: new(sync.Map), certsPrivateMap: new(sync.Map), certsPublicMap: new(sync.Map)}
	newDs.etcdStorage = NewEtcdKvStorage(StorageConfig{
		Endpoints:   strings.Split(param.EtcdEndpoints, ","),
		Prefix:      param.EtcdPrefix,
		UserName:    param.EtcdUser,
		Password:    param.EtcdPassword,
		DialTimeout: 5 * time.Second,
		LeaseTTL:    60 * time.Second,
	})
	if err = newDs.etcdStorage.Init(); err != nil {
		err = fmt.Errorf("init etcd connect fail,%s ", err.Error())
		return
	}
	ds = &newDs
	defer func() {
		if err != nil {
			log.Printf("register server error:%s,try to shutdown etcd connect...\n", err.Error())
			ds.etcdStorage.Shutdown()
		}
	}()
	// build server storage data
	ds.self = &EtcdServersData{
		Id:       uuid.New().String(),
		Type:     param.ServerType,
		CpuNum:   runtime.NumCPU(),
		CreateAt: time.Now().Format(time.RFC3339),
		Metadata: param.Metadata,
	}
	ds.self.Hostname, _ = os.Hostname()
	dataBytes, _ := json.Marshal(ds.self)
	// register to etcd discovery
	storageKey := fmt.Sprintf("/servers/%s/%s", param.ServerType, ds.self.Id)
	if err = ds.etcdStorage.SetSdServer(storageKey, dataBytes); err != nil {
		return
	}
	// init servers and certs data
	if err = ds.SyncServers(); err != nil {
		return
	}
	if err = ds.syncPrivateKeys(); err != nil {
		return
	}
	if err = ds.syncPublicKeys(); err != nil {
		return
	}
	// watch servers and certs change
	ds.watchServers()
	ds.watchPrivateKeys()
	ds.watchPublicKeys()
	go func() {
		time.Sleep(5 * time.Second)
		ds.SyncServers()
	}()
	return
}

func (ds *DiscoveryServer) SyncServers() (err error) {
	log.Println("start sync discovery servers")
	keys, values, getErr := ds.etcdStorage.GetAll("/servers")
	if getErr != nil {
		err = fmt.Errorf("get etcd servers data fail,%s", getErr.Error())
		return
	}
	serverTypeMap := make(map[string][]*EtcdServersData)
	for i, v := range values {
		serverObj := EtcdServersData{}
		if tmpErr := json.Unmarshal(v, &serverObj); tmpErr != nil {
			log.Printf("sync servers,json unmarshal etcd storage data:%s fail,%s ", keys[i], tmpErr.Error())
		} else {
			if existList, b := serverTypeMap[serverObj.Type]; b {
				serverTypeMap[serverObj.Type] = append(existList, &serverObj)
			} else {
				serverTypeMap[serverObj.Type] = []*EtcdServersData{&serverObj}
			}
		}
	}
	for k, v := range serverTypeMap {
		ds.serversMap.Store(k, v)
	}
	return
}

func (ds *DiscoveryServer) syncPrivateKeys() (err error) {
	keys, values, getErr := ds.etcdStorage.GetAll(fmt.Sprintf("/certs/private/%s", ds.self.Type))
	if getErr != nil {
		err = fmt.Errorf("get etcd private data fail,%s", getErr.Error())
		return
	}
	for i, v := range values {
		_, privateKeyName := splitEtcdPath(keys[i])
		if decodeBytes, decodeErr := base64.StdEncoding.DecodeString(string(v)); decodeErr == nil {
			ds.certsPrivateMap.Store(privateKeyName, decodeBytes)
		} else {
			ds.certsPrivateMap.Store(privateKeyName, v)
		}
	}
	return
}

func (ds *DiscoveryServer) syncPublicKeys() (err error) {
	keys, values, getErr := ds.etcdStorage.GetAll("/certs/public")
	if getErr != nil {
		err = fmt.Errorf("get etcd public data fail,%s", getErr.Error())
		return
	}
	publicKeyMap := make(map[string]map[string][]byte)
	for i, v := range values {
		serverType, publicKeyName := splitEtcdPath(keys[i])
		if em, b := publicKeyMap[serverType]; b {
			em[publicKeyName] = v
		} else {
			publicKeyMap[serverType] = make(map[string][]byte)
			publicKeyMap[serverType][publicKeyName] = v
		}
	}
	for k, v := range publicKeyMap {
		ds.certsPublicMap.Store(k, v)
	}
	return
}

func (ds *DiscoveryServer) watchServers() {
	ds.etcdStorage.Watch("/servers", func(op string, key string, value []byte, preValue []byte) {
		log.Printf("watch servers,op:%s key:%s value:%s \n", op, key, string(value))
		switch op {
		case "put":
			serverObj := EtcdServersData{}
			if tmpErr := json.Unmarshal(value, &serverObj); tmpErr != nil {
				log.Printf("watch servers put,json unmarshal etcd storage data:%s fail,%s ", key, tmpErr.Error())
			} else {
				if existList, ok := ds.serversMap.Load(serverObj.Type); ok {
					newServerList := []*EtcdServersData{&serverObj}
					for _, v := range existList.([]*EtcdServersData) {
						if v.Id != serverObj.Id {
							newServerList = append(newServerList, v)
						}
					}
					ds.serversMap.Store(serverObj.Type, newServerList)
				} else {
					ds.serversMap.Store(serverObj.Type, []*EtcdServersData{&serverObj})
				}
			}
		case "delete":
			serverType, serverId := splitEtcdPath(key)
			if existList, ok := ds.serversMap.Load(serverType); ok {
				newServerList := []*EtcdServersData{}
				for _, v := range existList.([]*EtcdServersData) {
					if v.Id != serverId {
						newServerList = append(newServerList, v)
					}
				}
				ds.serversMap.Store(serverType, newServerList)
			}
		}
	})
}

func (ds *DiscoveryServer) watchPrivateKeys() {
	ds.etcdStorage.Watch(fmt.Sprintf("/certs/private/%s", ds.self.Type), func(op string, key string, value []byte, preValue []byte) {
		log.Printf("watch private keys,op:%s key:%s valueLen:%d \n", op, key, len(value))
		switch op {
		case "put":
			_, privateKeyName := splitEtcdPath(key)
			if decodeBytes, decodeErr := base64.StdEncoding.DecodeString(string(value)); decodeErr == nil {
				ds.certsPrivateMap.Store(privateKeyName, decodeBytes)
			} else {
				ds.certsPrivateMap.Store(privateKeyName, value)
			}
		case "delete":
			_, privateKeyName := splitEtcdPath(key)
			ds.certsPrivateMap.Delete(privateKeyName)
		}
	})
}

func (ds *DiscoveryServer) watchPublicKeys() {
	ds.etcdStorage.Watch("/certs/public", func(op string, key string, value []byte, preValue []byte) {
		log.Printf("watch public keys,op:%s key:%s value:%s \n", op, key, string(value))
		switch op {
		case "put":
			serverType, publicKeyName := splitEtcdPath(key)
			if keyMap, ok := ds.certsPublicMap.Load(serverType); ok {
				newMap := make(map[string][]byte)
				newMap[publicKeyName] = value
				for k, v := range keyMap.(map[string][]byte) {
					if k != publicKeyName {
						newMap[k] = v
					}
				}
				ds.certsPublicMap.Store(serverType, newMap)
			} else {
				publicKeyMap := make(map[string][]byte)
				publicKeyMap[publicKeyName] = value
				ds.certsPublicMap.Store(serverType, publicKeyMap)
			}
		case "delete":
			serverType, publicKeyName := splitEtcdPath(key)
			if keyMap, ok := ds.certsPublicMap.Load(serverType); ok {
				newMap := make(map[string][]byte)
				for k, v := range keyMap.(map[string][]byte) {
					if k != publicKeyName {
						newMap[k] = v
					}
				}
				ds.certsPublicMap.Store(serverType, newMap)
			}
		}
	})
}

func (ds *DiscoveryServer) GetSelf() (self *EtcdServersData) {
	self = ds.self
	return
}

func (ds *DiscoveryServer) GetServersByType(serverType string) (targets []*EtcdServersData) {
	if servers, ok := ds.serversMap.Load(serverType); ok {
		targets = append(targets, servers.([]*EtcdServersData)...)
	}
	return
}

func (ds *DiscoveryServer) GetRandHttpServer(serverType string) (host, port string) {
	servers := ds.GetServersByType(serverType)
	if len(servers) == 0 {
		return
	}
	rand.Seed(time.Now().UnixNano())
	target := servers[rand.Intn(len(servers))]
	if target.Metadata != nil {
		host = target.Metadata.HttpHost
		port = target.Metadata.HttpPort
	}
	return
}

func (ds *DiscoveryServer) GetPrivateKey(keyName string) (result []byte) {
	if v, ok := ds.certsPrivateMap.Load(keyName); ok {
		result = v.([]byte)
	}
	return
}

func (ds *DiscoveryServer) GetPublicKey(serverType, keyName string) (result []byte) {
	if keyMap, ok := ds.certsPublicMap.Load(serverType); ok {
		result = keyMap.(map[string][]byte)[keyName]
	}
	return
}

func (ds *DiscoveryServer) Destroy() (result []byte) {
	log.Println("start destroy")
	ds.etcdStorage.Shutdown()
	return
}

func WaitProcessSignal() {
	sg := make(chan os.Signal)
	signal.Notify(sg, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM)
	s := <-sg
	log.Printf("get signal %s \n", s.String())
}

func splitEtcdPath(key string) (serverType, id string) {
	// example : data/servers/app/app-01
	pathList := strings.Split(key, "/")
	pathListLen := len(pathList)
	if pathListLen >= 2 {
		serverType = pathList[pathListLen-2]
		id = pathList[pathListLen-1]
	}
	return
}
