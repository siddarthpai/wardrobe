
# wardrobe

**wardrobe** is a fast, minimalist in-memory key-value store written in Go, inspired by Redis. Built from scratch, it supports core Redis functionalities like persistence, replication, transactions, streams, and TTL — all accessible via `redis-cli`.

---
(I named it **wardrobe** because it's an in-house Redis replica I built for an internal app at my company — and what better name for in-house storage than **wardrobe** ✨)
##  features

- 🧠 **In-Memory Storage** – blazing fast access
- 💾 **RDB Persistence** – saves data to `dump.rdb`
- 📡 **Master-Slave Replication** – supports replication config
- ⏳ **TTL Support** – with `EXPIRE` and time-based key eviction
- 🔄 **Transactions** – with `MULTI` and `EXEC`
- 🌊 **Streams** – with `XADD`
- ⚡ Fully **redis-cli compatible**

---

## how do you run this ? 

### 1. clone and build

```bash
git clone https://github.com/siddarthpai/wardrobe.git
cd wardrobe
go build -o wardrobe

```

### 2. run the server

```bash
./wardrobe --port 8000

```

----------

## using `redis-cli` to test

```bash
redis-cli -p 8000

```

Example commands:

```bash
SET hello "sidpai"
GET hello
EXPIRE hello 10
DEL hello

```

----------


## commands it supports : 

| Command                    | Description                                      |
|---------------------------|--------------------------------------------------|
| `SET key value`           | Set a key to a value                             |
| `GET key`                 | Get the value of a key                           |
| `DEL key`                 | Delete a key                                     |
| `EXPIRE key seconds`      | Set TTL for a key                                |
| `INCR key`                | Increment a key’s integer value                  |
| `MULTI` / `EXEC`          | Start and execute a transaction                  |
| `XADD stream key value`   | Add entry to a stream                            |
| `PING`                    | Ping the server                                  |


----------

## testing out replication

Run a master and replica instance:

```bash
# Master
./wardrobe --port 8000

# Replica (on a different port)
./wardrobe --port 8001 --replicaof 127.0.0.1 8000

```

----------

## testing out persistence

-   `dump.rdb` is auto-loaded on startup if available.
    
-   Future enhancements: snapshotting and AOF.
    

----------

## example of a transaction

```bash
MULTI
SET paisa 100
INCR paisa
EXEC
```

----------

## tweaking the configuration

You can tweak startup configs using `redis.conf` or CLI flags. TTL handling and replication configs are parsed via the config loader in `main.go`.

----------

## contributing

PRs, issues, and feature requests welcome! Open the door to contributions — just like we cache your data 😉

----------

## 📎 Repo

[🔗 GitHub → siddarthpai/wardrobe](https://github.com/siddarthpai/wardrobe)

----------
