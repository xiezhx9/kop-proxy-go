package utils

import (
	"github.com/hezhangjian/gox/syncx"
	"golang.org/x/time/rate"
	"time"
)

type KeyBasedRateLimiter struct {
	seconds    int
	times      int
	limiterMap syncx.Map[string, *rate.Limiter]
}

func NewKeyBasedRateLimiter(seconds, times int) *KeyBasedRateLimiter {
	return &KeyBasedRateLimiter{
		seconds: seconds,
		times:   times,
	}
}

func (k *KeyBasedRateLimiter) Acquire(key string) bool {
	rateLimiter, _ := k.limiterMap.LoadOrStore(key, rate.NewLimiter(rate.Every(time.Duration(k.seconds)*time.Second), k.times))
	return rateLimiter.Allow()
}

func (k *KeyBasedRateLimiter) Clean(key string) {
	k.limiterMap.Delete(key)
}
