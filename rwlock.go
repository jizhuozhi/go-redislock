package redislock

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	readLockSuffix   = ":read_lock"
	readQueueSuffix  = ":read_queue"
	writeLockSuffix  = ":write_lock"
	writeQueueSuffix = ":write_queue"

	notifyPrefix = "notify:"
)

var wlockScript = newScript(`
    local readLockKey     = KEYS[1]
    local _               = KEYS[2]
    local writeLockKey    = KEYS[3]
    local writeQueueKey   = KEYS[4]
    local lockExpiration  = ARGV[1]
    local threadId        = ARGV[2]

    if redis.call("EXISTS", writeLockKey) == 0 and redis.call("GET", readLockKey) == false then
        redis.call("SET", writeLockKey, threadId, "PX", lockExpiration)
        return "LOCKED"
    else 
        redis.call("RPUSH", writeQueueKey, threadId)
        return "QUEUED"
    end
`)

var wunlockScript = newScript(`
    local readLockKey     = KEYS[1]
    local readQueueKey    = KEYS[2]
    local writeLockKey    = KEYS[3]
    local writeQueueKey   = KEYS[4]
    local lockExpiration  = ARGV[1]
    local threadId        = ARGV[2]
    
    if redis.call("GET", writeLockKey) == threadId then
        redis.call("DEL", writeLockKey)
        local nextWaiter = redis.call("LPOP", writeQueueKey)
        if nextWaiter then
            redis.call("PUBLISH", "notify:"..nextWaiter, "GRANTED")
        else
            local readers = redis.call("LRANGE", readQueueKey, 0, -1)
            if #readers > 0 then
                redis.call("DEL", readQueueKey)
                redis.call("SET", readLockKey, #readers, "PX", lockExpiration)
                for _, reader in ipairs(readers) do
                    redis.call("PUBLISH", "notify:"..reader, "GRANTED")
                end
            end
        end
        return 1
    end
    return 0
`)

var rlockScript = newScript(`
    local readLockKey     = KEYS[1]
    local readQueueKey    = KEYS[2]
    local writeLockKey    = KEYS[3]
    local writeQueueKey   = KEYS[4]
    local lockExpiration  = ARGV[1]
    local threadId       = ARGV[2]

    if redis.call("EXISTS", writeLockKey) == 0 then
        redis.call("INCR", readLockKey)
        redis.call("PEXPIRE", readLockKey, lockExpiration)
        return "LOCKED"
    else
        redis.call("RPUSH", readQueueKey, threadId)
        return "QUEUED"
    end
`)

var runlockScript = newScript(`
    local readLockKey     = KEYS[1]
    local _               = KEYS[2]
    local writeLockKey    = KEYS[3]
    local writeQueueKey   = KEYS[4]
    local lockExpiration  = ARGV[1]
    local threadId       = ARGV[2]
    
    if redis.call("DECR", readLockKey) == 0 then
        redis.call("DEL", readLockKey)
        local nextWaiter = redis.call("LPOP", writeQueueKey)
        if nextWaiter then
            redis.call("SET", writeLockKey, nextWaiter, "PX", lockExpiration)
            redis.call("PUBLISH", "notify:"..nextWaiter, "GRANTED")
        end
    end
    return 1
`)

var notifyAllScript = newScript(`
    local queueKey = KEYS[1]
    
    local waiters = redis.call("LRANGE", queueKey, 0, -1)
    if #waiters > 0 then
        redis.call("DEL", queueKey)
        for _, waiter in ipairs(waiters) do
            redis.call("PUBLISH", "notify:"..waiter, "NOTIFIED")
        end
    end
`)

type Lock struct {
	client        *redis.Client
	name          string
	readLockKey   string
	readQueueKey  string
	writeLockKey  string
	writeQueueKey string
	expiration    time.Duration
}

func New(client *redis.Client, name string, expiration time.Duration) *Lock {
	return &Lock{
		client:        client,
		name:          name,
		readLockKey:   name + readLockSuffix,
		readQueueKey:  name + readQueueSuffix,
		writeLockKey:  name + writeLockSuffix,
		writeQueueKey: name + writeQueueSuffix,
		expiration:    expiration,
	}
}

func (l *Lock) Lock(ctx context.Context) error {
	threadId := getThreadId(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		res, err := wlockScript.exec(ctx, l.client, []string{l.readLockKey, l.readQueueKey, l.writeLockKey, l.writeQueueKey},
			l.expiration.Milliseconds(), threadId)
		if err != nil {
			return err
		}
		if res == "LOCKED" {
			return nil
		}
		msg, err := l.waitForAcquireLock(ctx, l.writeQueueKey, threadId)
		if err != nil {
			return err
		}
		if msg == "GRANTED" {
			return nil
		}
	}
}

func (l *Lock) Unlock(ctx context.Context) error {
	threadId := getThreadId(ctx)
	_, err := wunlockScript.exec(ctx, l.client, []string{l.readLockKey, l.readQueueKey, l.writeLockKey, l.writeQueueKey},
		l.expiration.Milliseconds(), threadId)
	return err
}

func (l *Lock) RLock(ctx context.Context) error {
	threadId := getThreadId(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		res, err := rlockScript.exec(ctx, l.client, []string{l.readLockKey, l.readQueueKey, l.writeLockKey, l.writeQueueKey},
			l.expiration.Milliseconds(), threadId)
		if err != nil {
			return err
		}
		if res == "LOCKED" {
			return nil
		}
		msg, err := l.waitForAcquireLock(ctx, l.readQueueKey, threadId)
		if err != nil {
			return err
		}
		if msg == "GRANTED" {
			return nil
		}
	}
}

func (l *Lock) RUnlock(ctx context.Context) error {
	threadId := getThreadId(ctx)
	_, err := runlockScript.exec(ctx, l.client, []string{l.readLockKey, l.readQueueKey, l.writeLockKey, l.writeQueueKey},
		l.expiration.Milliseconds(), threadId)
	return err
}

func (l *Lock) waitForAcquireLock(ctx context.Context, queue string, threadId string) (string, error) {
	key := notifyPrefix + threadId
	sub := l.client.Subscribe(ctx, key)

	defer sub.Close()
	defer l.client.Del(ctx, key)

	ch := sub.Channel()
	for {
		select {
		case msg := <-ch:
			return msg.Payload, nil
		case <-ctx.Done():
			_, _ = notifyAllScript.exec(context.WithoutCancel(ctx), l.client, []string{queue})
			return "", ctx.Err()
		}
	}
}
