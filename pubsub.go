package pubsub

import (
	"errors"
	"sync/atomic"

	pool "github.com/TikaFlow/worker-pool"
)

var (
	ErrBusClosed    = errors.New("pubsub: bus is closed")
	ErrInvalidTopic = errors.New("pubsub: invalid topic")
)

// SubscriptionID 订阅唯一标识
type SubscriptionID uint64

// Handler 消息回调函数类型
type Handler func(topic string, message any)

// subscription 内部订阅结构
type subscription struct {
	id      SubscriptionID
	topic   string
	handler Handler
	once    bool
}

type subscriptionMap map[SubscriptionID]*subscription

// dataBus 数据总线实例
type dataBus struct {
	subs     map[string]subscriptionMap
	idMap    map[SubscriptionID]string
	nextID   atomic.Uint64
	closed   atomic.Bool
	mgr      pool.Pool
	taskPool pool.Pool
}

// New 创建一个新的数据总线实例
func New() *dataBus {
	bus := &dataBus{
		idMap:    make(map[SubscriptionID]string),
		subs:     make(map[string]subscriptionMap),
		mgr:      pool.New(1, nil),
		taskPool: pool.New(8, nil),
	}

	return bus
}

// sub 内部订阅函数
func (bus *dataBus) sub(topic string, handler Handler, once bool) (SubscriptionID, error) {
	if bus.closed.Load() {
		return 0, ErrBusClosed
	}

	if !isValidTopic(topic) {
		return 0, ErrInvalidTopic
	}

	id := SubscriptionID(bus.nextID.Add(1))
	subTask := func() {
		sub := &subscription{
			id:      id,
			topic:   topic,
			handler: handler,
			once:    once,
		}
		bus.idMap[id] = topic
		if _, ok := bus.subs[topic]; !ok {
			bus.subs[topic] = make(subscriptionMap)
		}
		bus.subs[topic][id] = sub
	}
	bus.mgr.Add(subTask)

	return id, nil
}

// Subscribe 订阅主题，handler 将在 taskPool 中异步调用
func (bus *dataBus) Subscribe(topic string, handler Handler) (SubscriptionID, error) {
	return bus.sub(topic, handler, false)
}

// SubscribeOnce 单次订阅，收到一次消息后自动取消订阅
func (bus *dataBus) SubscribeOnce(topic string, handler Handler) (SubscriptionID, error) {
	return bus.sub(topic, handler, true)
}

// Publish 发布消息，topic 必须是具体的（不含通配符）
func (bus *dataBus) Publish(topic string, message any) error {
	if bus.closed.Load() {
		return ErrBusClosed
	}

	if hasWildcard(topic) {
		return ErrInvalidTopic
	}

	pubTask := func() {
		for pattern, idSub := range bus.subs {
			matched := false
			if !hasWildcard(pattern) {
				matched = pattern == topic
			} else {
				matched = topicMatch(pattern, topic)
			}

			if matched {
				for _, sub := range idSub {
					if sub.once {
						bus.unsubTask(sub.id)
					}
					bus.taskPool.Add(func() {
						sub.handler(topic, message)
					})
				}
			}

		}
	}
	bus.mgr.Add(pubTask)

	return nil
}

func (bus *dataBus) unsubTask(id SubscriptionID) {
	if topic, exists := bus.idMap[id]; exists {
		delete(bus.subs[topic], id)
		if len(bus.subs[topic]) == 0 {
			delete(bus.subs, topic)
		}
		delete(bus.idMap, id)
	}
}

// UnSubscribe 取消订阅
func (bus *dataBus) UnSubscribe(id SubscriptionID) {
	if bus.closed.Load() {
		return
	}

	bus.mgr.Add(func() {
		bus.unsubTask(id)
	})
}

// UnSubscribeAll 取消所有订阅
func (bus *dataBus) UnSubscribeAll() {
	unsubTask := func() {
		clear(bus.subs)
		clear(bus.idMap)
	}
	bus.mgr.Add(unsubTask)
}

// Close 关闭数据总线，实现 io.Closer 接口
func (bus *dataBus) Close() error {
	if bus.closed.Load() {
		return ErrBusClosed
	}

	bus.closed.Store(true)
	bus.mgr.Close()
	bus.taskPool.Close()

	return nil
}
