# sharded-lru

[![Go Report Card](https://goreportcard.com/badge/github.com/kevinskysunny/sharded-lru)](https://goreportcard.com/report/github.com/kevinskysunny/sharded-lru)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A compact, zero-dependency, high-performance generic sharded LRU cache for Go with lock-free eviction callbacks and zero data races.

> **Note**: This library was architected, peer-reviewed, and hardened end-to-end via the **KL Cockpit (klc) 5-Stage Multi-Agent Assembly Line** (DeepSeek V4 Pro ➔ Gemini 3.8 Flash ➔ MiniMax Code Plan ➔ Kimi K3 ➔ Sandbox Verification).

---

## ✨ Features

- **Generic & Type-Safe**: `ShardedLRU[K comparable, V any]` supports arbitrary comparable keys and values without interface boxing.
- **High Concurrency via Sharding**: Automatically partitions keys across $2^N$ independent shards, each with its own `sync.Mutex` to minimize lock contention.
- **Deadlock-Free Eviction Callbacks**: `OnEvict` callbacks run *outside* shard locks, safely supporting re-entrant calls into the cache.
- **Built-in Telemetry**: Atomic `Stats()` counter tracking `Hits`, `Misses`, and `Evicts`.
- **Zero External Dependencies**: Pure Go standard library (`hash/maphash`, `runtime`, `sync`).
- **Race-Condition Free**: Fully tested and verified under Go's race detector (`go test -race`).

---

## 📦 Installation

```bash
go get github.com/kevinskysunny/sharded-lru
```

---

## 🚀 Quickstart

```go
package main

import (
	"fmt"
	"github.com/kevinskysunny/sharded-lru"
)

func main() {
	// Initialize cache with capacity of 1,000 across 16 shards
	cache := lru.NewSharded[string, int](lru.Options[string, int]{
		Capacity: 1000,
		Shards:   16,
		OnEvict: func(key string, val int) {
			fmt.Printf("Evicted key=%s val=%d\n", key, val)
		},
	})

	// Put and Get
	cache.Put("user:101", 42)
	if val, ok := cache.Get("user:101"); ok {
		fmt.Printf("Got user:101 = %d\n", val)
	}

	// Read telemetry counters
	stats := cache.Stats()
	fmt.Printf("Stats: Hits=%d Misses=%d Evicts=%d\n",
		stats.Hits, stats.Misses, stats.Evicts)
}
```

---

## 🧪 Testing & Verification

Run tests with race detection:

```bash
go test -v -race ./...
```

Run benchmarks:

```bash
go test -bench=. -benchmem ./...
```

---

## 📄 License

MIT License © 2026 Kevin Smith
