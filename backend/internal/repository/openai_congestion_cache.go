package repository

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

func congestionKeys(group int64, session string) []string {
	key := buildSessionKey(group, session)
	return []string{key, key + ":congestion:pending", key + ":congestion:protected"}
}

func (c *gatewayCache) ReadCongestionSession(ctx context.Context, group int64, session string) (string, bool, error) {
	keys := congestionKeys(group, session)
	values, err := c.rdb.MGet(ctx, keys[1], keys[2]).Result()
	if err != nil {
		return "", false, err
	}
	pending, _ := values[0].(string)
	return pending, values[1] != nil, nil
}

var reserveCongestionScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] or redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
if redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3], 'NX') then return 1 end
return 0
`)

func (c *gatewayCache) ReserveCongestionSession(ctx context.Context, group int64, session string, from int64, pending string, ttl time.Duration) (bool, error) {
	n, err := reserveCongestionScript.Run(ctx, c.rdb, congestionKeys(group, session), from, pending, ttl.Milliseconds()).Int()
	return n == 1, err
}

var commitCongestionScript = redis.NewScript(`
if redis.call('GET', KEYS[2]) ~= ARGV[1] or redis.call('GET', KEYS[1]) ~= ARGV[2] then return 0 end
redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[4])
if tonumber(ARGV[5]) > 0 then redis.call('SET', KEYS[3], ARGV[3], 'PX', ARGV[5]) else redis.call('DEL', KEYS[3]) end
redis.call('DEL', KEYS[2])
if #KEYS > 3 then redis.call('SET', KEYS[4], ARGV[3], 'PX', ARGV[6]) end
return 1
`)

func (c *gatewayCache) CommitCongestionSession(ctx context.Context, group int64, session, pending string, from, to int64, ttl, protection time.Duration, legacy string, legacyTTL time.Duration) (bool, error) {
	keys := congestionKeys(group, session)
	if legacy != "" {
		keys = append(keys, buildSessionKey(group, legacy))
	}
	n, err := commitCongestionScript.Run(ctx, c.rdb, keys, pending, from, to, ttl.Milliseconds(), protection.Milliseconds(), legacyTTL.Milliseconds()).Int()
	return n == 1, err
}

var releaseCongestionScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`)

func (c *gatewayCache) ReleaseCongestionSession(ctx context.Context, group int64, session, pending string) error {
	return releaseCongestionScript.Run(ctx, c.rdb, []string{congestionKeys(group, session)[1]}, pending).Err()
}

var conditionalSessionBindingScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
local legacyMatches = #KEYS > 1 and redis.call('GET', KEYS[2]) == ARGV[1]
if (not current and (ARGV[1] == '0' or legacyMatches)) or current == ARGV[1] then
 redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
 if #KEYS > 1 then redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[4]) end
 return 1
end
return 0
`)

func (c *gatewayCache) SetSessionAccountIDIfUnchanged(ctx context.Context, group int64, session string, expected, target int64, ttl time.Duration, legacy string, legacyTTL time.Duration) (bool, error) {
	keys := []string{buildSessionKey(group, session)}
	if legacy != "" {
		keys = append(keys, buildSessionKey(group, legacy))
	}
	n, err := conditionalSessionBindingScript.Run(ctx, c.rdb, keys, expected, target, ttl.Milliseconds(), legacyTTL.Milliseconds()).Int()
	return n == 1, err
}

var conditionalSessionDeleteScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] or redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
redis.call('DEL', KEYS[1])
if #KEYS > 2 then redis.call('DEL', KEYS[3]) end
return 1
`)

func (c *gatewayCache) DeleteSessionAccountIDIfUnchanged(ctx context.Context, group int64, session string, expected int64, legacy string) (bool, error) {
	keys := congestionKeys(group, session)[:2]
	if legacy != "" {
		keys = append(keys, buildSessionKey(group, legacy))
	}
	n, err := conditionalSessionDeleteScript.Run(ctx, c.rdb, keys, expected).Int()
	return n == 1, err
}
