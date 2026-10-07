package lru

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func newTestCache(cap, nShards int) *ShardedLRU[int, int] {
	return NewSharded[int, int](Options[int, int]{
		Capacity: cap,
		Shards:   nShards,
	})
}

func TestSharded_PutGet(t *testing.T) {
	c := newTestCache(8, 2)
	if _, ok := c.Get(1); ok {
		t.Fatal("expected miss on empty cache")
	}
	if ev := c.Put(1, 100); ev {
		t.Fatal("did not expect eviction on first put")
	}
	if v, ok := c.Get(1); !ok || v != 100 {
		t.Fatalf("Get(1) = %v,%v want 100,true", v, ok)
	}
}

func TestSharded_UpdateDoesNotEvict(t *testing.T) {
	c := newTestCache(2, 1)
	c.Put(1, 1)
	c.Put(2, 2)
	if ev := c.Put(1, 11); ev {
		t.Fatal("updating existing key must not evict")
	}
	if c.Len() != 2 {
		t.Fatalf("Len=%d want 2", c.Len())
	}
	if v, _ := c.Get(1); v != 11 {
		t.Fatalf("Get(1)=%v want 11", v)
	}
}

func TestSharded_LRUEvictionOrder(t *testing.T) {
	c := newTestCache(3, 1)
	c.Put(1, 1)
	c.Put(2, 2)
	c.Put(3, 3)
	c.Get(1) // 1 becomes MRU; order is now 2(LRU), 3, 1(MRU)
	c.Put(4, 4) // should evict 2
	if _, ok := c.Get(2); ok {
		t.Fatal("expected key 2 to be evicted")
	}
	if v, ok := c.Get(1); !ok || v != 1 {
		t.Fatalf("Get(1)=%v want 1", v)
	}
}

func TestSharded_EvictCallbackFiresOutsideLock(t *testing.T) {
	var evicted int32
	var c *ShardedLRU[int, int]
	c = NewSharded[int, int](Options[int, int]{
		Capacity: 2,
		Shards:   1,
		OnEvict: func(k, v int) {
			atomic.AddInt32(&evicted, 1)
			// Re-entrancy check: calling cache methods inside OnEvict must not deadlock
			_ = c.Len()
		},
	})
	c.Put(1, 1)
	c.Put(2, 2)
	c.Put(3, 3) // evicts 1
	c.Put(4, 4) // evicts 2
	if got := atomic.LoadInt32(&evicted); got != 2 {
		t.Fatalf("evicted = %d, want 2", got)
	}
}

func TestSharded_HighConcurrency(t *testing.T) {
	const goroutines = 16
	const ops = 5000
	c := NewSharded[string, int](Options[string, int]{
		Capacity: 1000,
		Shards:   16,
	})

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				key := fmt.Sprintf("k-%d", (id*100+j)%500)
				c.Put(key, j)
				_, _ = c.Get(key)
			}
		}(i)
	}
	wg.Wait()

	stats := c.Stats()
	if stats.Hits == 0 {
		t.Fatal("expected non-zero hits")
	}
	if c.Len() > 1000 {
		t.Fatalf("cache size %d exceeded capacity 1000", c.Len())
	}
}

func BenchmarkSharded_ConcurrentGetPut(b *testing.B) {
	c := NewSharded[int, int](Options[int, int]{
		Capacity: 10000,
		Shards:   16,
	})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Put(i%5000, i)
			_, _ = c.Get(i % 5000)
			i++
		}
	})
}
