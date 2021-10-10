package tcr

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/streadway/amqp"
)

// ConnectionPool houses the pool of RabbitMQ connections.
type ConnectionPool struct {
	Config               PoolConfig        //连接池配置
	uri                  string            //连接URL
	heartbeatInterval    time.Duration     //心跳时间间隔
	connectionTimeout    time.Duration     //连接超时
	connections          *queue.Queue      //用于保存已创建连接队列
	channels             chan *ChannelHost //保存已创建channel集合
	connectionID         uint64            //连接标记
	poolRWLock           *sync.RWMutex     //连接池读写锁
	flaggedConnections   map[uint64]bool   //标记已衰退(不可用)连接集合
	sleepOnErrorInterval time.Duration     //连接创建错误时延时时间
	errorHandler         func(error)       //异常处理回调
}

// NewConnectionPool creates hosting structure for the ConnectionPool.
func NewConnectionPool(config *PoolConfig) (*ConnectionPool, error) {

	if config.Heartbeat == 0 || config.ConnectionTimeout == 0 {
		return nil, errors.New("connection pool heartbeat or connection timeout can't be 0")
	}

	if config.MaxConnectionCount == 0 {
		return nil, errors.New("connection pool max connection count can't be 0")
	}

	cp := &ConnectionPool{
		Config:               *config,
		uri:                  config.URI,
		heartbeatInterval:    time.Duration(config.Heartbeat) * time.Second,         //单位秒
		connectionTimeout:    time.Duration(config.ConnectionTimeout) * time.Second, //单位秒
		connections:          queue.New(int64(config.MaxConnectionCount)),           // possible overflow error
		channels:             make(chan *ChannelHost, config.MaxCacheChannelCount),
		poolRWLock:           &sync.RWMutex{},
		flaggedConnections:   make(map[uint64]bool),
		sleepOnErrorInterval: time.Duration(config.SleepOnErrorInterval) * time.Millisecond,
	}

	if ok := cp.initializeConnections(); !ok {
		return nil, errors.New("initialization failed during connection creation")
	}

	return cp, nil
}

// NewConnectionPoolWithErrorHandler creates hosting structure for the ConnectionPool.
func NewConnectionPoolWithErrorHandler(config *PoolConfig, errorHandler func(error)) (*ConnectionPool, error) {

	if config.Heartbeat == 0 || config.ConnectionTimeout == 0 {
		return nil, errors.New("connectionpool heartbeat or connectiontimeout can't be 0")
	}

	if config.MaxConnectionCount == 0 {
		return nil, errors.New("connectionpool maxconnectioncount can't be 0")
	}

	cp := &ConnectionPool{
		Config:               *config,
		uri:                  config.URI,
		heartbeatInterval:    time.Duration(config.Heartbeat) * time.Second,
		connectionTimeout:    time.Duration(config.ConnectionTimeout) * time.Second,
		connections:          queue.New(int64(config.MaxConnectionCount)), // possible overflow error
		channels:             make(chan *ChannelHost, config.MaxCacheChannelCount),
		poolRWLock:           &sync.RWMutex{},
		flaggedConnections:   make(map[uint64]bool),
		sleepOnErrorInterval: time.Duration(config.SleepOnErrorInterval) * time.Millisecond,
		errorHandler:         errorHandler,
	}

	if ok := cp.initializeConnections(); !ok {
		return nil, errors.New("initialization failed during connection creation")
	}

	return cp, nil
}

//initializeConnections 连接池初始化
func (cp *ConnectionPool) initializeConnections() bool {

	cp.connectionID = 0 //连接池连接ID由0开始自增
	cp.connections = queue.New(int64(cp.Config.MaxConnectionCount))

	for i := uint64(0); i < cp.Config.MaxConnectionCount; i++ {

		connectionHost, err := NewConnectionHost(
			cp.uri,
			cp.Config.ApplicationName+"-"+strconv.FormatUint(cp.connectionID, 10),
			cp.connectionID,
			cp.heartbeatInterval,
			cp.connectionTimeout,
			cp.Config.TLSConfig)

		if err != nil {
			cp.handleError(err)
			return false
		}

		if err = cp.connections.Put(connectionHost); err != nil {
			cp.handleError(err)
			return false
		}

		cp.connectionID++
	}

	for i := uint64(0); i < cp.Config.MaxCacheChannelCount; i++ { //初始化时先创建连接数量为 最大缓存连接数 一致
		cp.channels <- cp.createCacheChannel(i) //创建新的连接
	}

	return true
}

// GetConnection gets a connection based on whats in the ConnectionPool (blocking under bad network conditions).
// Flowcontrol (blocking) or transient network outages will pause here until cleared.
// Uses the SleepOnErrorInterval to pause between retries.
func (cp *ConnectionPool) GetConnection() (*ConnectionHost, error) {

	connHost, err := cp.getConnectionFromPool() //从连接池中获取一个连接
	if err != nil {                             // errors on bad data in the queue
		cp.handleError(err)
		return nil, err
	}

	cp.verifyHealthyConnection(connHost)

	return connHost, nil
}

//getConnectionFromPool 从连接池(队列)中获取连接
func (cp *ConnectionPool) getConnectionFromPool() (*ConnectionHost, error) {

	// Pull from the queue.
	// Pauses here indefinitely if the queue is empty.
	structs, err := cp.connections.Get(1) //TODO: 队列Get(1)原因？
	if err != nil {
		return nil, err
	}

	connHost, ok := structs[0].(*ConnectionHost)
	if !ok {
		return nil, errors.New("invalid struct type found in ConnectionPool queue")
	}

	return connHost, nil
}

//verifyHealthyConnection 验证当前连接状态是否健康
func (cp *ConnectionPool) verifyHealthyConnection(connHost *ConnectionHost) {

	healthy := true
	select {
	case <-connHost.Errors:
		healthy = false
	default:
		break
	}

	flagged := cp.isConnectionFlagged(connHost.ConnectionID) //通过连接ID检测当前连接是否标记为不可用

	// Between these three states we do our best to determine that a connection is dead in the various lifecycles.
	if flagged || !healthy || connHost.Connection.IsClosed( /* atomic */ ) {
		cp.triggerConnectionRecovery(connHost)
	}

	connHost.PauseOnFlowControl()
}

//triggerConnectionRecovery 当连接被标记为不可用(衰退)、连接存在错误、可当前连接已关闭时，将触发连接重连
func (cp *ConnectionPool) triggerConnectionRecovery(connHost *ConnectionHost) {

	// InfiniteLoop: Stay here till we reconnect. 无限循环，直至连接创建成功
	for {
		ok := connHost.Connect()
		if !ok {
			if cp.sleepOnErrorInterval > 0 {
				time.Sleep(cp.sleepOnErrorInterval)
			}
			continue
		}
		break
	}

	// Flush any pending errors. 清除当前连接之前全部异常，并重新将其标记为可用状态
	for {
		select {
		case <-connHost.Errors:
		default:
			cp.unflagConnection(connHost.ConnectionID) //重新标记为可用
			return
		}
	}
}

// ReturnConnection puts the connection back in the queue and flag it for error.
// This helps maintain a Round Robin on Connections and their resources.
func (cp *ConnectionPool) ReturnConnection(connHost *ConnectionHost, flag bool) {

	if flag { //将连接添加至不可用集合中  map[uint64]bool
		cp.flagConnection(connHost.ConnectionID)
	}

	_ = cp.connections.Put(connHost) //将当前连接添加至已创建队列，不管可用还是不可用
}

// GetChannelFromPool gets a cached ackable channel from the Pool if they exist or creates a channel.
// A non-acked channel is always a transient channel.
// Blocking if Ackable is true and the cache is empty.
// If you want a transient Ackable channel (un-managed), use CreateChannel directly.
func (cp *ConnectionPool) GetChannelFromPool() *ChannelHost {

	return <-cp.channels
}

// ReturnChannel returns a Channel.
// If Channel is not a cached channel, it is simply closed here.
// If Cache Channel, we check if erred, new Channel is created instead and then returned to the cache.
func (cp *ConnectionPool) ReturnChannel(chanHost *ChannelHost, erred bool) {

	// If called by user with the wrong channel don't add a non-managed channel back to the channel cache.
	if chanHost.CachedChannel {
		if erred {
			cp.reconnectChannel(chanHost) // <- blocking operation
		} else {
			chanHost.FlushConfirms()
		}

		cp.channels <- chanHost
		return
	}

	go func(*ChannelHost) {
		defer func() { _ = recover() }()

		chanHost.Close()
	}(chanHost)
}

func (cp *ConnectionPool) reconnectChannel(chanHost *ChannelHost) {

	// InfiniteLoop: Stay here till we reconnect.
	for {
		cp.verifyHealthyConnection(chanHost.connHost) // <- blocking operation

		err := chanHost.MakeChannel() // Creates a new channel and flushes internal buffers automatically.
		if err != nil {
			cp.handleError(err)
			continue
		}
		break
	}
}

// createCacheChannel allows you create a cached ChannelHost which helps wrap Amqp Channel functionality.
func (cp *ConnectionPool) createCacheChannel(id uint64) *ChannelHost {

	// InfiniteLoop: Stay till we have a good channel.
	for {
		connHost, err := cp.GetConnection()
		if err != nil {
			cp.handleError(err)
			continue
		}

		chanHost, err := NewChannelHost(connHost, id, connHost.ConnectionID, true, true)
		if err != nil {
			cp.handleError(err)
			cp.ReturnConnection(connHost, true) //标记不可用，将会添加至flagConnection集合中
			continue                            //继续创建连接
		}

		cp.ReturnConnection(connHost, false) //标记可用
		return chanHost
	}
}

// GetTransientChannel allows you create an unmanaged amqp Channel with the help of the ConnectionPool.
func (cp *ConnectionPool) GetTransientChannel(ackable bool) *amqp.Channel {

	// InfiniteLoop: Stay till we have a good channel.
	for {
		connHost, err := cp.GetConnection()
		if err != nil {
			cp.handleError(err)
			continue
		}

		channel, err := connHost.Connection.Channel()
		if err != nil {
			cp.handleError(err)
			cp.ReturnConnection(connHost, true)
			continue
		}

		cp.ReturnConnection(connHost, false)

		if ackable {
			err := channel.Confirm(false)
			if err != nil {
				cp.handleError(err)
				continue
			}
		}
		return channel
	}
}

// UnflagConnection flags that connection as usable in the future.
func (cp *ConnectionPool) unflagConnection(connectionID uint64) {
	cp.poolRWLock.Lock()
	defer cp.poolRWLock.Unlock()
	cp.flaggedConnections[connectionID] = false
}

// FlagConnection flags that connection as non-usable in the future.
func (cp *ConnectionPool) flagConnection(connectionID uint64) {
	cp.poolRWLock.Lock()
	defer cp.poolRWLock.Unlock()
	cp.flaggedConnections[connectionID] = true
}

// IsConnectionFlagged 检查连接是否已被标记为已衰退(不可用)  checks to see if the connection has been flagged for removal.
func (cp *ConnectionPool) isConnectionFlagged(connectionID uint64) bool {
	cp.poolRWLock.RLock()
	defer cp.poolRWLock.RUnlock()
	if flagged, ok := cp.flaggedConnections[connectionID]; ok {
		return flagged
	}

	return false
}

// Shutdown closes all connections in the ConnectionPool and resets the Pool to pre-initialized state.
func (cp *ConnectionPool) Shutdown() {

	if cp == nil {
		return
	}

	wg := &sync.WaitGroup{}
ChannelFlushLoop:
	for {
		select {
		case chanHost := <-cp.channels:
			wg.Add(1)
			// Started receiving panics on Channel.Close()
			go func(*ChannelHost) {
				defer wg.Done()
				defer func() { _ = recover() }()

				chanHost.Close()
			}(chanHost)

		default:
			break ChannelFlushLoop
		}
	}
	wg.Wait()

	for !cp.connections.Empty() {
		items, _ := cp.connections.Get(cp.connections.Len())

		for _, item := range items {
			wg.Add(1)

			connectionHost := item.(*ConnectionHost)

			// Started receiving panics on Connection.Close()
			go func(*ConnectionHost) {
				defer wg.Done()
				defer func() { _ = recover() }()

				if !connectionHost.Connection.IsClosed() {
					connectionHost.Connection.Close()
				}
			}(connectionHost)

		}
	}

	wg.Wait()

	cp.connections = queue.New(int64(cp.Config.MaxConnectionCount))
	cp.flaggedConnections = make(map[uint64]bool)
	cp.connectionID = 0
}

func (cp *ConnectionPool) handleError(err error) {
	if cp.errorHandler != nil {
		cp.errorHandler(err)
	}
	if cp.sleepOnErrorInterval > 0 {
		time.Sleep(cp.sleepOnErrorInterval)
	}
}
