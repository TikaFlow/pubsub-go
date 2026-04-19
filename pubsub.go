package pubsub

import (
    "errors"
    "io"
    "runtime"
)

var (
    ErrBusClosed     = errors.New("pubsub: bus is closed")
    ErrNotConcrete   = errors.New("pubsub: publish topic must be concrete (no wildcards)")
    ErrNotFound      = errors.New("pubsub: subscription not found")
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

// 内部请求类型
type subRequest struct {
    topic   string
    handler Handler
    once    bool
    resp    chan subResponse
}

type subResponse struct {
    id  SubscriptionID
    err error
}

type unsubRequest struct {
    id   SubscriptionID
    resp chan error
}

type unsubAllRequest struct {
    resp chan struct{}
}

type pubRequest struct {
    topic   string
    message any
}

// Bus 数据总线实例
type Bus struct {
    subCh      chan *subRequest
    unsubCh    chan *unsubRequest
    unsubAllCh chan *unsubAllRequest
    pubCh      chan *pubRequest
    closeCh    chan struct{}
    doneCh     chan struct{}
    sem        chan struct{}
}

// New 创建一个新的数据总线实例
func New() *Bus {
    maxConc := runtime.NumCPU() * 2
    if maxConc < 8 {
        maxConc = 8
    }

    b := &Bus{
        subCh:      make(chan *subRequest, 64),
        unsubCh:    make(chan *unsubRequest, 64),
        unsubAllCh: make(chan *unsubAllRequest, 16),
        pubCh:      make(chan *pubRequest, 128),
        closeCh:    make(chan struct{}),
        doneCh:     make(chan struct{}),
        sem:        make(chan struct{}, maxConc),
    }

    go b.run()

    return b
}

// run 事件循环，在独立 goroutine 中运行
func (b *Bus) run() {
    exactSubs := make(map[string][]*subscription)
    wildSubs := make([]*subscription, 0)
    nextID := SubscriptionID(0)

    for {
        select {
        case req := <-b.subCh:
            if err := validateTopic(req.topic); err != nil {
                req.resp <- subResponse{err: err}
                continue
            }
            nextID++
            sub := &subscription{
                id:      nextID,
                topic:   req.topic,
                handler: req.handler,
                once:    req.once,
            }
            if hasWildcard(req.topic) {
                wildSubs = append(wildSubs, sub)
            } else {
                exactSubs[req.topic] = append(exactSubs[req.topic], sub)
            }
            req.resp <- subResponse{id: nextID}

        case req := <-b.unsubCh:
            found := false
            for topic, subs := range exactSubs {
                for i, sub := range subs {
                    if sub.id == req.id {
                        exactSubs[topic] = append(subs[:i], subs[i+1:]...)
                        if len(exactSubs[topic]) == 0 {
                            delete(exactSubs, topic)
                        }
                        found = true
                        break
                    }
                }
                if found {
                    break
                }
            }
            if !found {
                for i, sub := range wildSubs {
                    if sub.id == req.id {
                        wildSubs = append(wildSubs[:i], wildSubs[i+1:]...)
                        found = true
                        break
                    }
                }
            }
            if found {
                req.resp <- nil
            } else {
                req.resp <- ErrNotFound
            }

        case req := <-b.unsubAllCh:
            for topic := range exactSubs {
                delete(exactSubs, topic)
            }
            wildSubs = wildSubs[:0]
            req.resp <- struct{}{}

        case req := <-b.pubCh:
            matched := make([]*subscription, 0)

            if subs, ok := exactSubs[req.topic]; ok {
                for _, sub := range subs {
                    matched = append(matched, sub)
                }
            }

            for _, sub := range wildSubs {
                if topicMatch(sub.topic, req.topic) {
                    matched = append(matched, sub)
                }
            }

            var onceIDs []SubscriptionID
            for _, sub := range matched {
                if sub.once {
                    onceIDs = append(onceIDs, sub.id)
                }
            }

            for _, id := range onceIDs {
                for topic, subs := range exactSubs {
                    for i, sub := range subs {
                        if sub.id == id {
                            exactSubs[topic] = append(subs[:i], subs[i+1:]...)
                            if len(exactSubs[topic]) == 0 {
                                delete(exactSubs, topic)
                            }
                            break
                        }
                    }
                }
                for i, sub := range wildSubs {
                    if sub.id == id {
                        wildSubs = append(wildSubs[:i], wildSubs[i+1:]...)
                        break
                    }
                }
            }

            for _, sub := range matched {
                b.sem <- struct{}{}
                go func(h Handler, t string, m any) {
                    defer func() {
                        recover()
                        <-b.sem
                    }()
                    h(t, m)
                }(sub.handler, req.topic, req.message)
            }

        case <-b.closeCh:
            close(b.doneCh)
            return
        }
    }
}

// isClosed 检查总线是否已关闭
func (b *Bus) isClosed() bool {
    select {
    case <-b.doneCh:
        return true
    default:
        return false
    }
}

// Subscribe 订阅主题，handler 将在独立 goroutine 中异步调用
func (b *Bus) Subscribe(topic string, handler Handler) (SubscriptionID, error) {
    if b.isClosed() {
        return 0, ErrBusClosed
    }
    req := &subRequest{
        topic:   topic,
        handler: handler,
        once:    false,
        resp:    make(chan subResponse, 1),
    }
    b.subCh <- req
    resp := <-req.resp
    return resp.id, resp.err
}

// SubscribeOnce 单次订阅，收到一次消息后自动取消订阅
func (b *Bus) SubscribeOnce(topic string, handler Handler) (SubscriptionID, error) {
    if b.isClosed() {
        return 0, ErrBusClosed
    }
    req := &subRequest{
        topic:   topic,
        handler: handler,
        once:    true,
        resp:    make(chan subResponse, 1),
    }
    b.subCh <- req
    resp := <-req.resp
    return resp.id, resp.err
}

// Publish 发布消息，topic 必须是具体的（不含通配符）
func (b *Bus) Publish(topic string, message any) error {
    if b.isClosed() {
        return ErrBusClosed
    }
    if !isConcreteTopic(topic) {
        return ErrNotConcrete
    }
    b.pubCh <- &pubRequest{topic: topic, message: message}
    return nil
}

// UnSubscribe 取消订阅
func (b *Bus) UnSubscribe(id SubscriptionID) error {
    if b.isClosed() {
        return ErrBusClosed
    }
    req := &unsubRequest{
        id:   id,
        resp: make(chan error, 1),
    }
    b.unsubCh <- req
    return <-req.resp
}

// UnSubscribeAll 取消所有订阅
func (b *Bus) UnSubscribeAll() {
    if b.isClosed() {
        return
    }
    req := &unsubAllRequest{
        resp: make(chan struct{}, 1),
    }
    b.unsubAllCh <- req
    <-req.resp
}

// Close 关闭数据总线，实现 io.Closer 接口
func (b *Bus) Close() error {
    if b.isClosed() {
        return ErrBusClosed
    }
    close(b.closeCh)
    <-b.doneCh
    return nil
}

// 确认 Bus 实现了 io.Closer 接口
var _ io.Closer = (*Bus)(nil)

// 全局默认实例
var defaultBus = New()

// Subscribe 使用默认总线订阅主题
func Subscribe(topic string, handler Handler) (SubscriptionID, error) {
    return defaultBus.Subscribe(topic, handler)
}

// SubscribeOnce 使用默认总线单次订阅
func SubscribeOnce(topic string, handler Handler) (SubscriptionID, error) {
    return defaultBus.SubscribeOnce(topic, handler)
}

// Publish 使用默认总线发布消息
func Publish(topic string, message any) error {
    return defaultBus.Publish(topic, message)
}

// UnSubscribe 使用默认总线取消订阅
func UnSubscribe(id SubscriptionID) error {
    return defaultBus.UnSubscribe(id)
}

// UnSubscribeAll 使用默认总线取消所有订阅
func UnSubscribeAll() {
    defaultBus.UnSubscribeAll()
}

// Close 关闭默认总线
func Close() error {
    return defaultBus.Close()
}
