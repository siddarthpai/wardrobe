# wardrobe

**wardrobe** is a fast, minimalist in-memory key-value store written in Go, inspired by Redis. Built from scratch, it supports core Redis functionalities like persistence, replication, transactions, streams, and TTL — all accessible via `redis-cli`.

---

(I named it **wardrobe** because it's an in-house Redis replica I built for an internal app at my company — and what better name for in-house storage than **wardrobe** ✨)

## features

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

---

## using `redis-cli` to test

```bash
redis-cli -p 8000
```

Example commands:
checking if it works :
![pingpong](https://i.postimg.cc/G2zt1QZT/Screenshot-2025-07-12-at-7-32-30-PM.png)
working with strings :
![Strings](https://i.postimg.cc/k5KRLWJW/Screenshot-2025-07-12-at-7-26-29-PM.png)

working with lists :
![Lists](https://i.postimg.cc/4d17xZhd/Screenshot-2025-07-12-at-7-27-32-PM.png)

working with sets :
![Sets](https://i.postimg.cc/ydK3KWxt/Screenshot-2025-07-12-at-7-27-39-PM.png)

using transactions :
![transactions](https://i.postimg.cc/7PWPMfD4/Screenshot-2025-07-12-at-7-34-56-PM.png)

using expiration on values :
![ttl](https://i.postimg.cc/sDWm96qJ/Screenshot-2025-07-12-at-7-43-30-PM.png)

---

## commands it supports :

| Command                 | Description                     |
| ----------------------- | ------------------------------- |
| `SET key value`         | Set a key to a value            |
| `GET key`               | Get the value of a key          |
| `DEL key`               | Delete a key                    |
| `PX key seconds`        | Set TTL for a key               |
| `INCR key`              | Increment a key’s integer value |
| `MULTI` / `EXEC`        | Start and execute a transaction |
| `XADD stream key value` | Add entry to a stream           |
| `PING`                  | Ping the server                 |

---

## testing out replication

starting the master instance :
![](https://i.postimg.cc/FFdz6jTz/Screenshot-2025-07-12-at-7-36-17-PM.png)
creating the replica instance :
![](https://i.postimg.cc/pVwrMpSZ/Screenshot-2025-07-12-at-7-37-45-PM.png)

---

## testing out persistence

- `dump.rdb` is auto-loaded on startup if available.

---

## future enhancements

- supporting more data types
- snapshotting of rdb and AOF.

---

## contributing

PRs, issues, and feature requests welcome!

---

## references

[GitHub → siddarthpai/wardrobe](https://github.com/siddarthpai/wardrobe)
[Redis Documentation](https://redis.io/docs/latest/)

---
