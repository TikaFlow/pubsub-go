package pubsub

import (
	"sync"
	"testing"
	"time"
)

func TestTopicMatch(t *testing.T) {
	cases := []struct {
		pattern string
		topic   string
		match   bool
	}{
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/b/d", false},
		{"a/+/c", "a/b/c", true},
		{"a/+/c", "a/x/c", true},
		{"a/+/c", "a/b/d", false},
		{"a/+/+", "a/b/c", true},
		{"#", "a/b/c", true},
		{"#", "a", true},
		{"#", "", true},
		{"a/#", "a/b/c", true},
		{"a/#", "a/b/c/d", true},
		{"a/#", "a", true},
		{"a/#", "b", false},
		{"a/+/+/d", "a/b/c/d", true},
		{"a/+/+/d", "a/b/x/d", true},
		{"a/+/+/d", "a/b/c/e", false},
	}

	for _, tc := range cases {
		res := topicMatch(tc.pattern, tc.topic)
		if res != tc.match {
			t.Errorf("pattern: %q, topic: %q, expected %v, got %v", tc.pattern, tc.topic, tc.match, res)
		}
	}
}

func TestHasWildcard(t *testing.T) {
	cases := []struct {
		topic  string
		expect bool
	}{
		{"a/b/c", false},
		{"a/+/c", true},
		{"#", true},
		{"a/#", true},
		{"", false},
	}

	for _, tc := range cases {
		res := hasWildcard(tc.topic)
		if res != tc.expect {
			t.Errorf("topic: %q, expected %v, got %v", tc.topic, tc.expect, res)
		}
	}
}

func TestIsValidTopic(t *testing.T) {
	cases := []struct {
		topic  string
		expect bool
	}{
		{"a/b/c", true},
		{"a/+/c", true},
		{"a/#", true},
		{"#", true},
		{"+/a", true},
		{"+/+", true},
		{"", false},
		{"a//b", false},
		{"a/#/b", false},
		{"a/b#", false},
		{"a/+/b#", false},
	}

	for _, tc := range cases {
		res := isValidTopic(tc.topic)
		if res != tc.expect {
			t.Errorf("topic: %q, expected %v, got %v", tc.topic, tc.expect, res)
		}
	}
}

func TestPublishSubscribe(t *testing.T) {
	bus := New()
	defer bus.Close()

	var wg sync.WaitGroup
	received := make(map[string]string)
	var mu sync.Mutex

	handler := func(topic string, message any) {
		defer wg.Done()
		mu.Lock()
		received[topic] = message.(string)
		mu.Unlock()
	}

	wg.Add(2)
	id1, err := bus.Subscribe("test/a", handler)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := bus.Subscribe("test/+", handler)
	if err != nil {
		t.Fatal(err)
	}

	_ = id2

	err = bus.Publish("test/a", "hello")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}

	mu.Lock()
	if received["test/a"] != "hello" {
		t.Errorf("expected received['test/a'] == 'hello', got %q", received["test/a"])
	}
	mu.Unlock()

	bus.UnSubscribe(id1)
}

func TestSubscribeOnce(t *testing.T) {
	bus := New()
	defer bus.Close()

	var wg sync.WaitGroup
	count := 0
	var mu sync.Mutex

	handler := func(topic string, message any) {
		defer wg.Done()
		mu.Lock()
		count++
		mu.Unlock()
	}

	wg.Add(1)
	_, err := bus.SubscribeOnce("once/test", handler)
	if err != nil {
		t.Fatal(err)
	}

	err = bus.Publish("once/test", "msg1")
	if err != nil {
		t.Fatal(err)
	}
	err = bus.Publish("once/test", "msg2")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}

	mu.Lock()
	if count != 1 {
		t.Errorf("expected count == 1, got %d", count)
	}
	mu.Unlock()
}

func TestUnSubscribeAll(t *testing.T) {
	bus := New()
	defer bus.Close()

	var wg sync.WaitGroup
	count := 0
	var mu sync.Mutex

	handler := func(topic string, message any) {
		defer wg.Done()
		mu.Lock()
		count++
		mu.Unlock()
	}

	for i := 0; i < 5; i++ {
		_, err := bus.Subscribe("all/test", handler)
		if err != nil {
			t.Fatal(err)
		}
	}

	wg.Add(5)
	err := bus.Publish("all/test", "msg")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}

	bus.UnSubscribeAll()

	time.Sleep(100 * time.Millisecond)
}

func TestPanicRecovery(t *testing.T) {
	bus := New()
	defer bus.Close()

	var wg sync.WaitGroup
	ok := false

	handlerPanic := func(topic string, message any) {
		defer wg.Done()
		panic("test panic")
	}

	handlerOk := func(topic string, message any) {
		defer wg.Done()
		ok = true
	}

	wg.Add(2)
	_, _ = bus.Subscribe("panic/test", handlerPanic)
	_, _ = bus.Subscribe("panic/test", handlerOk)

	_ = bus.Publish("panic/test", "msg")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}

	if !ok {
		t.Error("expected ok == true (ok handler should still execute after panic handler)")
	}
}

func TestWildcardSubscribe(t *testing.T) {
	bus := New()
	defer bus.Close()

	var wg sync.WaitGroup
	count := 0
	var mu sync.Mutex

	handler := func(topic string, message any) {
		defer wg.Done()
		mu.Lock()
		count++
		mu.Unlock()
	}

	wg.Add(4)
	_, _ = bus.Subscribe("a/+", handler)
	_, _ = bus.Subscribe("a/#", handler)

	_ = bus.Publish("a/b", "msg")
	_ = bus.Publish("a/c", "msg")
	_ = bus.Publish("x/y", "msg")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}

	mu.Lock()
	if count != 4 {
		t.Errorf("expected count == 4, got %d", count)
	}
	mu.Unlock()
}

func TestGlobalDefaultBus(t *testing.T) {
	var wg sync.WaitGroup
	received := false

	handler := func(topic string, message any) {
		defer wg.Done()
		received = true
	}

	wg.Add(1)
	id, err := Subscribe("global/test", handler)
	if err != nil {
		t.Fatal(err)
	}

	err = Publish("global/test", "msg")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}

	if !received {
		t.Error("expected received == true")
	}

	UnSubscribe(id)
}

func TestCloseBus(t *testing.T) {
	bus := New()

	err := bus.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, err = bus.Subscribe("a", func(string, any) {})
	if err != ErrBusClosed {
		t.Errorf("expected ErrBusClosed, got %v", err)
	}

	err = bus.Publish("a", "msg")
	if err != ErrBusClosed {
		t.Errorf("expected ErrBusClosed, got %v", err)
	}
}

func TestUnSubscribeNotFound(t *testing.T) {
	bus := New()
	defer bus.Close()

	bus.UnSubscribe(9999)
	bus.UnSubscribe(1)
}

func TestSubscribeInvalidTopic(t *testing.T) {
	bus := New()
	defer bus.Close()

	_, err := bus.Subscribe("", func(string, any) {})
	if err != ErrInvalidTopic {
		t.Errorf("expected ErrInvalidTopic, got %v", err)
	}

	_, err = bus.Subscribe("a//b", func(string, any) {})
	if err != ErrInvalidTopic {
		t.Errorf("expected ErrInvalidTopic, got %v", err)
	}
}

func TestPublishInvalidTopic(t *testing.T) {
	bus := New()
	defer bus.Close()

	err := bus.Publish("a/+", "msg")
	if err != ErrInvalidTopic {
		t.Errorf("expected ErrInvalidTopic, got %v", err)
	}

	err = bus.Publish("a/#", "msg")
	if err != ErrInvalidTopic {
		t.Errorf("expected ErrInvalidTopic, got %v", err)
	}
}

func BenchmarkSubscribe(b *testing.B) {
	bus := New()
	defer bus.Close()

	handler := func(string, any) {}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Subscribe("bench/test", handler)
	}
}

func BenchmarkPublish(b *testing.B) {
	bus := New()
	defer bus.Close()

	handler := func(string, any) {}
	for i := 0; i < 100; i++ {
		bus.Subscribe("bench/test", handler)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish("bench/test", "msg")
	}
}

func BenchmarkPublishWildcard(b *testing.B) {
	bus := New()
	defer bus.Close()

	handler := func(string, any) {}
	bus.Subscribe("bench/#", handler)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish("bench/test/sub", "msg")
	}
}

func BenchmarkConcurrentPublish(b *testing.B) {
	bus := New()
	defer bus.Close()

	handler := func(string, any) {}
	for i := 0; i < 100; i++ {
		bus.Subscribe("bench/test", handler)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bus.Publish("bench/test", "msg")
		}
	})
}
