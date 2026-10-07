package lru

import (
	"hash/maphash"
	"runtime"
	"sync"
	"sync/atomic"
)

// HashFunc maps a key to a uint64 hash.
type HashFunc[K comparable] func(K) uint64

// EvictFunc is invoked when an entry is evicted due to capacity pressure.
// It is called outside shard locks to prevent deadlocks.
type EvictFunc[K comparable, V any] func(key K, value V)

// Options configures a ShardedLRU.
type Options[K comparable, V any] struct {
	Capacity int
	Shards   int
	Hash     HashFunc[K]
	OnEvict  EvictFunc[K, V]
}

type entry[K comparable, V any] struct {
	key   K
	value V
	hash  uint64
	prev  *entry[K, V]
	next  *entry[K, V]
}

type shard[K comparable, V any] struct {
	mu    sync.Mutex
	items map[uint64]*entry[K, V]
	head  *entry[K, V] // sentinel; head.next is MRU
	tail  *entry[K, V] // sentinel; tail.prev is LRU
	size  int
	cap   int
}

func newShard[K comparable, V any](cap int) *shard[K, V] {
	h := &entry[K, V]{}
	t := &entry[K, V]{}
	h.next = t
	t.prev = h
	return &shard[K, V]{
		items: make(map[uint64]*entry[K, V]),
		head:  h,
		tail:  t,
		cap:   cap,
	}
}

func (s *shard[K, V]) remove(e *entry[K, V]) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev = nil
	e.next = nil
}

func (s *shard[K, V]) insertMRU(e *entry[K, V]) {
	e.next = s.head.next
	e.prev = s.head
	s.head.next.prev = e
	s.head.next = e
}

// Stats captures cache counters.
type Stats struct {
	Size   int
	Cap    int
	Shards int
	Hits   uint64
	Misses uint64
	Evicts uint64
}

// ShardedLRU is a high-concurrency sharded LRU cache.
type ShardedLRU[K comparable, V any] struct {
	shards    []*shard[K, V]
	shardMask uint64
	hash      HashFunc[K]
	seed      maphash.Seed
	onEvict   EvictFunc[K, V]
	hits      uint64
	misses    uint64
	evicts    uint64
}

func defaultShards() int {
	gp := runtime.GOMAXPROCS(0)
	if gp <= 1 {
		return 1
	}
	n := 1
	for n < gp && n < 256 {
		n <<= 1
	}
	return n
}

func clampShards(n int) int {
	if n <= 1 {
		return 1
	}
	if n > 256 {
		return 256
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// NewSharded creates a new high-performance generic sharded LRU.
func NewSharded[K comparable, V any](opts Options[K, V]) *ShardedLRU[K, V] {
	totalCap := opts.Capacity
	if totalCap < 0 {
		totalCap = 0
	}

	nShards := opts.Shards
	if nShards <= 0 {
		nShards = defaultShards()
	}
	nShards = clampShards(nShards)

	base := totalCap / nShards
	rem := totalCap % nShards
	caps := make([]int, nShards)
	for i := range caps {
		caps[i] = base
		if i < rem {
			caps[i]++
		}
	}

	c := &ShardedLRU[K, V]{
		shards:    make([]*shard[K, V], nShards),
		shardMask: uint64(nShards - 1),
		hash:      opts.Hash,
		seed:      maphash.MakeSeed(),
		onEvict:   opts.OnEvict,
	}
	for i := range c.shards {
		c.shards[i] = newShard[K, V](caps[i])
	}
	return c
}

func splitmix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	if x == 0 {
		return 1
	}
	return x
}

func (c *ShardedLRU[K, V]) hashKey(k K) (idx uint64, key uint64) {
	var h uint64
	if c.hash != nil {
		h = c.hash(k)
	} else {
		h = maphash.Comparable(c.seed, k)
	}
	if h == 0 {
		h = splitmix64(0x9e3779b97f4a7c15)
	}
	return h & c.shardMask, h
}

// Get looks up a key, marking it as most-recently-used on hit.
func (c *ShardedLRU[K, V]) Get(k K) (V, bool) {
	idx, key := c.hashKey(k)
	s := c.shards[idx]
	s.mu.Lock()
	if e, ok := s.items[key]; ok {
		s.remove(e)
		s.insertMRU(e)
		v := e.value
		s.mu.Unlock()
		atomic.AddUint64(&c.hits, 1)
		return v, true
	}
	s.mu.Unlock()
	atomic.AddUint64(&c.misses, 1)
	var z V
	return z, false
}

// Put inserts or updates an entry, returning true if an existing entry was evicted.
func (c *ShardedLRU[K, V]) Put(k K, v V) bool {
	idx, key := c.hashKey(k)
	s := c.shards[idx]
	s.mu.Lock()
	if e, ok := s.items[key]; ok {
		e.value = v
		s.remove(e)
		s.insertMRU(e)
		s.mu.Unlock()
		return false
	}

	e := &entry[K, V]{key: k, value: v, hash: key}
	s.items[key] = e
	s.insertMRU(e)
	s.size++

	if s.size > s.cap && s.cap > 0 {
		victim := s.tail.prev
		s.remove(victim)
		delete(s.items, victim.hash)
		s.size--
		s.mu.Unlock()
		atomic.AddUint64(&c.evicts, 1)
		if c.onEvict != nil {
			c.onEvict(victim.key, victim.value)
		}
		return true
	}
	s.mu.Unlock()
	return false
}

// Delete removes a key from the cache.
func (c *ShardedLRU[K, V]) Delete(k K) bool {
	idx, key := c.hashKey(k)
	s := c.shards[idx]
	s.mu.Lock()
	if e, ok := s.items[key]; ok {
		s.remove(e)
		delete(s.items, key)
		s.size--
		s.mu.Unlock()
		if c.onEvict != nil {
			c.onEvict(e.key, e.value)
		}
		return true
	}
	s.mu.Unlock()
	return false
}

// Len returns total items stored across all shards.
func (c *ShardedLRU[K, V]) Len() int {
	n := 0
	for _, s := range c.shards {
		s.mu.Lock()
		n += s.size
		s.mu.Unlock()
	}
	return n
}

// Cap returns configured total capacity.
func (c *ShardedLRU[K, V]) Cap() int {
	n := 0
	for _, s := range c.shards {
		s.mu.Lock()
		n += s.cap
		s.mu.Unlock()
	}
	return n
}

// Stats returns hit/miss/eviction counters.
func (c *ShardedLRU[K, V]) Stats() Stats {
	var size_, cap_ int
	for _, s := range c.shards {
		s.mu.Lock()
		size_ += s.size
		cap_ += s.cap
		s.mu.Unlock()
	}
	return Stats{
		Size:   size_,
		Cap:    cap_,
		Shards: len(c.shards),
		Hits:   atomic.LoadUint64(&c.hits),
		Misses: atomic.LoadUint64(&c.misses),
		Evicts: atomic.LoadUint64(&c.evicts),
	}
}

// Clear removes all entries and triggers OnEvict outside lock if set.
func (c *ShardedLRU[K, V]) Clear() {
	for _, s := range c.shards {
		var drained []*entry[K, V]
		s.mu.Lock()
		if c.onEvict != nil {
			for _, e := range s.items {
				drained = append(drained, e)
			}
		}
		s.items = make(map[uint64]*entry[K, V])
		s.head.next = s.tail
		s.tail.prev = s.head
		s.size = 0
		s.mu.Unlock()

		for _, e := range drained {
			c.onEvict(e.key, e.value)
		}
	}
}
