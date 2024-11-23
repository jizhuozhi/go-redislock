package redislock

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestSingleThread(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	l := New(rdb, "single_thread", 30*time.Second)
	defer assertLockStats(t, rdb, l)

	ctx := WithThreadId(context.Background(), "thread_id_1")

	err := l.RLock(ctx)
	assert.NoError(t, err)

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err = l.Lock(timeoutCtx)
	assert.Error(t, err)

	l.RUnlock(ctx)
	err = l.Lock(ctx)
	assert.NoError(t, err)
	l.Unlock(ctx)
}

func TestMultiThread(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	l := New(rdb, "multi_thread", 30*time.Second)
	defer assertLockStats(t, rdb, l)

	ctx := context.Background()
	wg := sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			ctx := WithThreadId(ctx, fmt.Sprintf("thread_id_%d", i))
			err := l.RLock(ctx)
			assert.NoError(t, err)
			defer l.RUnlock(ctx)
			time.Sleep(500 * time.Millisecond)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := WithThreadId(ctx, "writer_thread")
		time.Sleep(100 * time.Millisecond)
		err := l.Lock(ctx)
		assert.NoError(t, err)
		defer l.Unlock(ctx)
		time.Sleep(100 * time.Millisecond)
	}()
	wg.Wait()
}

func TestWritePriority(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	l := New(rdb, "write_priority", 30*time.Second)
	defer assertLockStats(t, rdb, l)

	m := sync.Mutex{}
	var priority []string

	ctx := context.Background()
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			threadId := fmt.Sprintf("thread_id_%d", i)
			ctx := WithThreadId(ctx, threadId)
			time.Sleep(time.Duration(i*100) * time.Millisecond)
			err := l.RLock(ctx)
			assert.NoError(t, err)
			defer l.RUnlock(ctx)
			m.Lock()
			defer m.Unlock()
			priority = append(priority, threadId)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		threadId := "writer_thread"
		ctx := WithThreadId(ctx, threadId)
		time.Sleep(300 * time.Millisecond)
		err := l.Lock(ctx)
		assert.NoError(t, err)
		defer l.Unlock(ctx)
		m.Lock()
		defer m.Unlock()
		priority = append(priority, threadId)
	}()
	wg.Wait()

	idx := -1
	for i, threadId := range priority {
		if threadId == "writer_thread" {
			idx = i
			break
		}
	}
	assert.True(t, idx > 0 && idx < len(priority))
}

func assertLockStats(t *testing.T, rdb *redis.Client, l *Lock) {
	ctx := context.Background()
	_, err := rdb.Get(ctx, l.readLockKey).Result()
	assert.ErrorIs(t, redis.Nil, err)
	res, err := rdb.LRange(ctx, l.readQueueKey, 0, -1).Result()
	assert.Empty(t, res)
	_, err = rdb.Get(ctx, l.writeLockKey).Result()
	assert.ErrorIs(t, redis.Nil, err)
	res, err = rdb.LRange(ctx, l.writeQueueKey, 0, -1).Result()
	assert.Empty(t, res)
}
