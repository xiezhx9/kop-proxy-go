package utils

import (
	"golang.org/x/time/rate"
	"sync"
	"time"
)

type KeyBasedRateLimiter struct {
	seconds    int
	times      int
	limiterMap sync.Map
}

func NewKeyBasedRateLimiter(seconds, times int) *KeyBasedRateLimiter {
	return &KeyBasedRateLimiter{
		seconds: seconds,
		times:   times,
	}
}

func (k *KeyBasedRateLimiter) Acquire(key string) bool {
	rateLimiter, _ := k.limiterMap.LoadOrStore(key, rate.NewLimiter(rate.Every(time.Duration(k.seconds)*time.Second), k.times))
	return rateLimiter.(*rate.Limiter).Allow()
}

func (k *KeyBasedRateLimiter) Clean(key string) {
	k.limiterMap.Delete(key)
}
