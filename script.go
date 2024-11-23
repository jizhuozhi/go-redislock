package redislock

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"strings"

	"github.com/go-redis/redis/v8"
)

type LuaScript struct {
	script string
	sha1   string
}

func newScript(script string) *LuaScript {
	hash := sha1.New()
	hash.Write([]byte(script))
	sum := hex.EncodeToString(hash.Sum(nil))
	return &LuaScript{
		script: script,
		sha1:   sum,
	}
}

func (s *LuaScript) exec(ctx context.Context, client *redis.Client, keys []string, args ...interface{}) (interface{}, error) {
	res, err := client.EvalSha(ctx, s.sha1, keys, args...).Result()
	if err != nil {
		if strings.Contains(err.Error(), "NOSCRIPT") {
			res, err = client.Eval(ctx, s.script, keys, args...).Result()
		}
	}
	return res, err
}
