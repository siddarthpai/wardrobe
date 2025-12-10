package store

import (
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/siddarthpai/wardrobe/rdb"
	"github.com/siddarthpai/wardrobe/respgo"
)

type Connection struct {
	Conn       net.Conn
	TxnStarted bool
	TxnQueue   [][]string
}

type Info struct {
	Role             string
	MasterIP         string
	MasterPort       string
	MasterReplId     string
	MasterReplOffSet int
	MasterConn       net.Conn
	slaves           []net.Conn
	Port             string
	flags            map[string]string
}

type StreamEntry struct {
	Id   string
	Pair map[string]string
}

type KVStore struct {
	Info           Info
	store          map[string]string
	expiryMap      map[string]chan int
	lists          map[string][]string
	sets           map[string]map[string]struct{}
	AckCh          chan int
	ProcessedWrite bool
	StreamXCh      chan []byte
	Stream         map[string][]StreamEntry
}

func (kv *KVStore) Set(key, value string, expiry int) {

	if expiry >= 0 {
		timeout := time.After(time.Duration(expiry) * time.Millisecond)
		go kv.handleExpiry(timeout, key)
	}

	kv.store[key] = value
}

func New() *KVStore {

	return &KVStore{
		Info: Info{
			Role:             "master",
			MasterReplId:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			MasterReplOffSet: 0,
			Port:             "8000",
		},
		store:     make(map[string]string),
		expiryMap: make(map[string]chan int),
		lists:     make(map[string][]string),
		sets:      make(map[string]map[string]struct{}),
		AckCh:     make(chan int),
		Stream:    make(map[string][]StreamEntry),
		StreamXCh: make(chan []byte),
	}
}

func (kv *KVStore) LoadFromRDB(dump *rdb.DumpParser) {
	if len(dump.Databases) < 1 {
		return
	}
	kv.store = dump.Databases[0].KeyValues
	for _, rec := range dump.Databases[0].TTLRecords {
		kv.store[rec.Key] = rec.Value
		delay := time.Duration(int64(rec.ExpireAt)-time.Now().UnixMilli()) * time.Millisecond
		go kv.handleExpiry(time.After(delay), rec.Key)
	}
}

func (kv *KVStore) handleExpiry(timeout <-chan time.Time, key string) {
	stop := make(chan int)
	kv.expiryMap[key] = stop
	select {
	case <-timeout:
		delete(kv.store, key)
	case <-stop:
	}
}

func (kv *KVStore) LoadRDB(master net.Conn) {
	fmt.Println("started loading rdb from master... ")
	parser := respgo.NewParser(master)

	msg, err := parser.ParseMessage()
	if err != nil {
		fmt.Println("error reading RDB payload:", err)
		return
	}
	data, ok := msg.([]byte)
	if !ok {
		fmt.Printf("expected bulk string, got %T\n", msg)
		return
	}

	dump, err := rdb.NewParserFromBytes(data)
	if err != nil {
		panic(err)
	}
	kv.LoadFromRDB(dump)
	fmt.Println("finished loading rdb from master...")
}

func (kv *KVStore) HandleConnection(conn Connection, parser *respgo.RespParser) {
	defer conn.Conn.Close()
	for {
		msg, err := parser.ParseMessage()
		if err == io.EOF {
			break
		} else if err != nil {
			panic("parse error: " + err.Error())
		}

		args, ok := msg.([]string)
		if !ok || len(args) == 0 {
			continue
		}

		// transaction queuing
		cmd := strings.ToUpper(args[0])

		txnCmds := []string{"EXEC", "DISCARD"}
		if conn.TxnStarted && !slices.Contains(txnCmds, cmd) {
			conn.TxnQueue = append(conn.TxnQueue, args)
			conn.Conn.Write([]byte("+QUEUED\r\n"))
			continue
		}

		reply := kv.processCommand(args, &conn)

		if kv.Info.Role == "slave" {
			if conn.Conn == kv.Info.MasterConn && cmd == "REPLCONF" && strings.ToUpper(args[1]) == "GETACK" {
				conn.Conn.Write(reply)
			}
			kv.Info.MasterReplOffSet += len(respgo.EncodeArray(args))
		} else {
			conn.Conn.Write(reply)
			if cmd == "SET" {
				for _, slave := range kv.Info.slaves {
					slave.Write(respgo.EncodeArray(args))
				}
			}
		}
	}
}

func (kv *KVStore) processCommand(args []string, connection *Connection) []byte {

	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "PING":
		return []byte("+Ping-a-Ding-Dong\r\n")
	case "ECHO":
		return respgo.EncodeBulkString(args[1])
	case "SET":
		key, val := args[1], args[2]
		ex := -1
		if len(args) > 4 {
			if n, err := strconv.Atoi(args[4]); err == nil {
				ex = n
			}
		}
		kv.Set(key, val, ex)
		return []byte("+SET DONE\r\n")
	case "GET":
		key := args[1]
		if v, ok := kv.store[key]; ok {
			return respgo.EncodeBulkString(v)
		}
		return []byte("$-1\r\n")
	case "DEL":
		key := args[1]
		if _, found := kv.store[key]; !found {
			return respgo.EncodeInteger(0)
		}
		delete(kv.store, key)
		if stop, ok := kv.expiryMap[key]; ok {
			stop <- 1
		}
		return respgo.EncodeInteger(1)

	case "CONFIG":
		if len(args) >= 3 && strings.ToUpper(args[1]) == "GET" {
			if v, ok := kv.Info.flags[args[2]]; ok {
				return respgo.EncodeArray([]string{args[2], v})
			}
		}
		return []byte("+OK\r\n")
	case "KEYS":
		var all []string
		for k := range kv.store {
			all = append(all, k)
		}
		return respgo.EncodeArray(all)
	case "INFO":

		var sb strings.Builder
		sb.WriteString("# Server\r\n")
		sb.WriteString(fmt.Sprintf("role:%s\r\n", kv.Info.Role))
		sb.WriteString(fmt.Sprintf("master_replid:%s\r\n", kv.Info.MasterReplId))
		sb.WriteString(fmt.Sprintf("master_repl_offset:%d\r\n", kv.Info.MasterReplOffSet))
		sb.WriteString("\r\n")

		return respgo.EncodeBulkString(sb.String())
	case "REPLCONF":
		sub := strings.ToUpper(args[1])
		switch sub {
		case "GETACK":
			return respgo.EncodeArray([]string{"REPLCONF", "ACK", strconv.Itoa(kv.Info.MasterReplOffSet)})
		case "ACK":
			kv.AckCh <- 1
			return []byte("+OK\r\n")
		default:
			return []byte("+OK\r\n")
		}
	case "PSYNC":
		header := fmt.Sprintf("+FULLRESYNC %s %d\r\n", kv.Info.MasterReplId, kv.Info.MasterReplOffSet)
		rdbBlob, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
		payload := append([]byte(fmt.Sprintf("$%d\r\n", len(rdbBlob))), rdbBlob...)
		kv.Info.slaves = append(kv.Info.slaves, connection.Conn)
		return append([]byte(header), payload...)
	case "WAIT":
		req, _ := strconv.Atoi(args[1])
		to, _ := strconv.Atoi(args[2])
		if len(kv.store) == 0 {
			return respgo.EncodeInteger(len(kv.Info.slaves))
		}
		for _, s := range kv.Info.slaves {
			go s.Write(respgo.EncodeArray([]string{"REPLCONF", "GETACK", "*"}))
		}
		acks := 0
		timer := time.After(time.Duration(to) * time.Millisecond)
	WAIT_LOOP:
		for acks < req {

			select {
			case <-kv.AckCh:
				acks++
			case <-timer:
				break WAIT_LOOP
			}
		}
		return respgo.EncodeInteger(acks)

	case "TYPE":
		key := args[1]
		switch {
		case kv.store[key] != "":
			return []byte("+string\r\n")
		case kv.lists[key] != nil:
			return []byte("+list\r\n")
		case kv.sets[key] != nil:
			return []byte("+set\r\n")
		default:
			return []byte("+none\r\n")
		}
	case "XADD":
		// generate an ID if the user passed "*"
		if args[2] == "*" {
			args[2] = fmt.Sprintf("%d-0", time.Now().UnixMilli())
		}

		streamKey := args[1]
		lastEntries := kv.Stream[streamKey]
		if len(lastEntries) > 0 {
			last := lastEntries[len(lastEntries)-1]
			lastParts := strings.Split(last.Id, "-")
			currParts := strings.Split(args[2], "-")

			lastTime, _ := strconv.Atoi(lastParts[0])
			currTime, _ := strconv.Atoi(currParts[0])
			lastSeq, _ := strconv.Atoi(lastParts[1])

			// if user specified "*", bump sequence or reset
			if currParts[1] == "*" {
				if currTime == lastTime {
					args[2] = fmt.Sprintf("%d-%d", lastTime, lastSeq+1)
				} else if currTime == 0 {
					args[2] = fmt.Sprintf("0-1")
				} else {
					args[2] = fmt.Sprintf("%d-0", currTime)
				}
				currParts = strings.Split(args[2], "-")
				currTime, _ = strconv.Atoi(currParts[0])
			}

			currSeq, _ := strconv.Atoi(currParts[1])
			// error checks
			if currTime < 1 && currSeq < 1 {
				return []byte("-ERR The ID specified in XADD must be greater than 0-0\r\n")
			}
			if lastTime > currTime ||
				(lastTime == currTime && lastSeq >= currSeq) {
				return []byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
			}
		} else {
			// empty stream, turn "*,*" into "0-1"
			parts := strings.Split(args[2], "-")
			if parts[1] == "*" {
				args[2] = "0-1"
			}
		}

		// build the StreamEntry
		se := StreamEntry{
			Id:   args[2],
			Pair: make(map[string]string, (len(args)-3)/2),
		}
		for i := 3; i < len(args); i += 2 {
			se.Pair[args[i]] = args[i+1]
		}

		// append into the store
		kv.Stream[streamKey] = append(kv.Stream[streamKey], se)

		// reply with the entry ID
		reply := respgo.EncodeBulkString(args[2])
		select {
		case kv.StreamXCh <- reply:
		default:
		}
		return reply

	case "LPUSH":
		key := args[1]
		if _, exists := kv.sets[key]; exists {
			return []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
		}
		for _, v := range args[2:] {
			kv.lists[key] = append([]string{v}, kv.lists[key]...)
		}
		return respgo.EncodeInteger(len(kv.lists[key]))

	case "LRANGE":
		key := args[1]
		start, _ := strconv.Atoi(args[2])
		stop, _ := strconv.Atoi(args[3])
		list, ok := kv.lists[key]
		if !ok {
			return respgo.EncodeArray([]string{})
		}
		if start < 0 {
			start = len(list) + start
		}
		if stop < 0 {
			stop = len(list) + stop
		}
		if start < 0 {
			start = 0
		}
		if stop >= len(list) {
			stop = len(list) - 1
		}
		if start > stop {
			return respgo.EncodeArray([]string{})
		}
		return respgo.EncodeArray(list[start : stop+1])

	case "SADD":
		key := args[1]
		if _, exists := kv.lists[key]; exists {
			return []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
		}
		if kv.sets[key] == nil {
			kv.sets[key] = make(map[string]struct{})
		}
		added := 0
		for _, m := range args[2:] {
			if _, ok := kv.sets[key][m]; !ok {
				kv.sets[key][m] = struct{}{}
				added++
			}
		}
		return respgo.EncodeInteger(added)

	case "SMEMBERS":
		key := args[1]
		set, ok := kv.sets[key]
		if !ok {
			return respgo.EncodeArray([]string{})
		}
		var mems []string
		for m := range set {
			mems = append(mems, m)
		}
		return respgo.EncodeArray(mems)

	case "XRANGE":
		key := args[1]

		endArg := args[3]
		if endArg == "+" {
			endArg = fmt.Sprintf("%d-%d", math.MaxInt64, math.MaxInt64)
		}
		startParts := strings.Split(args[2], "-")
		endParts := strings.Split(endArg, "-")

		startTime, _ := strconv.Atoi(startParts[0])
		startSeq := 0
		if len(startParts) > 1 {
			startSeq, _ = strconv.Atoi(startParts[1])
		}
		endTime, _ := strconv.Atoi(endParts[0])
		endSeq := -1
		if len(endParts) > 1 {
			endSeq, _ = strconv.Atoi(endParts[1])
		}

		var nestedEntries [][]byte
		for _, se := range kv.Stream[key] {
			parts := strings.Split(se.Id, "-")
			t, _ := strconv.Atoi(parts[0])
			s, _ := strconv.Atoi(parts[1])
			if (t > startTime || (t == startTime && s >= startSeq)) &&
				(t < endTime || (t == endTime && s <= endSeq)) {
				// build [ ID, [ field, value, … ] ]
				idFrame := respgo.EncodeBulkString(se.Id)

				// and one _array_ for the field/value pairs
				var fv []string
				for f, v := range se.Pair {
					fv = append(fv, f, v)
				}
				pairFrame := respgo.EncodeArray(fv)

				// now wrap those two raw frames as a nested array
				nested := respgo.EncodeRawArray(idFrame, pairFrame)
				nestedEntries = append(nestedEntries, nested)
			}
		}
		return respgo.EncodeRawArray(nestedEntries...)
	case "XREAD":

		block := strings.ToLower(args[1]) == "block"
		waitMs := 0
		offset := 2
		if block {
			waitMs, _ = strconv.Atoi(args[2])
			offset = 4
		}

		parts := args[offset:]
		half := len(parts) / 2
		keys, ids := parts[:half], parts[half:]

		if block {
			if waitMs > 0 {
				select {
				case <-kv.StreamXCh:
				case <-time.After(time.Duration(waitMs) * time.Millisecond):
					return []byte("$-1\r\n")
				}
			} else {
				<-kv.StreamXCh
			}
		}

		var outer [][]byte
		for i, key := range keys {
			var groupItems [][]byte

			if ids[i] == "$" {

				se := kv.Stream[key][len(kv.Stream[key])-1]
				idF := respgo.EncodeBulkString(se.Id)
				var fv []string
				for f, v := range se.Pair {
					fv = append(fv, f, v)
				}
				pairF := respgo.EncodeArray(fv)
				groupItems = append(groupItems, respgo.EncodeRawArray(idF, pairF))

			} else {

				thr := strings.Split(ids[i], "-")
				thrT, _ := strconv.Atoi(thr[0])
				thrS, _ := strconv.Atoi(thr[1])
				for _, se := range kv.Stream[key] {
					parts := strings.Split(se.Id, "-")
					t, _ := strconv.Atoi(parts[0])
					s, _ := strconv.Atoi(parts[1])
					if t > thrT || (t == thrT && s > thrS) {
						idF := respgo.EncodeBulkString(se.Id)
						var fv []string
						for f, v := range se.Pair {
							fv = append(fv, f, v)
						}
						pairF := respgo.EncodeArray(fv)
						groupItems = append(groupItems, respgo.EncodeRawArray(idF, pairF))
					}
				}
			}

			keyF := respgo.EncodeBulkString(key)
			inner := respgo.EncodeRawArray(groupItems...)
			outer = append(outer, respgo.EncodeRawArray(keyF, inner))
		}

		return respgo.EncodeRawArray(outer...)
	case "INCR":
		key := args[1]
		v, ok := kv.store[key]
		if !ok {
			kv.store[key] = "1"
		} else {
			n, err := strconv.Atoi(v)
			if err != nil {
				return []byte("-ERR value is not an integer or out of range\r\n")
			}
			kv.store[key] = strconv.Itoa(n + 1)
		}
		n, err := strconv.Atoi(kv.store[key])
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}
		return respgo.EncodeInteger(n)

	case "MULTI":
		connection.TxnStarted = true
		return []byte("+OK\r\n")
	case "EXEC":
		if !connection.TxnStarted {
			return []byte("-ERR EXEC without MULTI\r\n")
		}
		var replies []string
		for _, queued := range connection.TxnQueue {
			b := kv.processCommand(queued, connection)
			replies = append(replies, string(b))
		}
		connection.TxnStarted = false
		connection.TxnQueue = nil
		return respgo.EncodeArray(replies)
	case "DISCARD":
		if !connection.TxnStarted {
			return []byte("-ERR DISCARD without MULTI\r\n")
		}
		connection.TxnStarted = false
		connection.TxnQueue = nil
		return []byte("+OK\r\n")

	default:
		return []byte("-ERR unknown command\r\n")
	}
}

var OP_CODES = []string{"FF", "FE", "FD", "FC", "FB", "FA"}

func (kv *KVStore) SendHandshake(master net.Conn, parser *respgo.RespParser) {
	steps := [][]string{
		{"PING"},
		{"REPLCONF", "listening-port", kv.Info.Port},
		{"REPLCONF", "capa", "psync2"},
		{"PSYNC", "?", "-1"},
	}
	for _, cmd := range steps {
		master.Write(respgo.EncodeArray(cmd))
		msg, _ := parser.ParseMessage()
		fmt.Printf("↩ %v\n", msg)
	}
	kv.Info.MasterConn = master
}

func (kv *KVStore) ExpectRDBFile(parser *respgo.RespParser) {
	msg, err := parser.ParseMessage()
	if err != nil {
		fmt.Println("nothing to read")
		return
	}
	data, ok := msg.([]byte)
	if !ok {
		fmt.Printf("expected bulk, got %T\n", msg)
		return
	}
	fmt.Println(string(data))
}

func (kv *KVStore) HandleReplication() {
	master, err := net.Dial("tcp", kv.Info.MasterIP+":"+kv.Info.MasterPort)
	if err != nil {
		panic(err)
	}
	parser := respgo.NewParser(master)
	kv.SendHandshake(master, parser)
	go kv.HandleConnection(Connection{Conn: master}, parser)
}

func (kv *KVStore) ParseCommandLine() {
	flags := make(map[string]string)
	args := os.Args[1:]

	for i := 0; i < len(args); i++ {
		if !strings.HasPrefix(args[i], "--") {
			continue
		}
		key := strings.TrimPrefix(args[i], "--")

		switch key {
		case "replicaof":
			if i+2 < len(args) &&
				!strings.HasPrefix(args[i+1], "--") &&
				!strings.HasPrefix(args[i+2], "--") {
				kv.Info.Role = "slave"
				kv.Info.MasterIP = args[i+1]
				kv.Info.MasterPort = args[i+2]
				i += 2
				continue
			}
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") {
				parts := strings.SplitN(args[i+1], ":", 2)
				if len(parts) != 2 {
					fmt.Fprintf(os.Stderr, "invalid replicaof argument: %s\n", args[i+1])
					os.Exit(1)
				}
				kv.Info.Role = "slave"
				kv.Info.MasterIP = parts[0]
				kv.Info.MasterPort = parts[1]
				i++
				continue
			}
			fmt.Fprintf(os.Stderr, "invalid replicaof argument\n")
			os.Exit(1)

		default:
			// All other flags are single‐valued: --flag value
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") {
				flags[key] = args[i+1]
				i++
			}
		}
	}

	if port, ok := flags["port"]; ok {
		kv.Info.Port = port
	}

	if dir, ok := flags["dir"]; ok {
		if fname, ok2 := flags["dbfilename"]; ok2 {
			fullPath := path.Join(dir, fname)
			dump, err := rdb.NewParser(fullPath)
			if err == nil {
				if err := dump.Parse(); err != nil {
					panic(err)
				}
				kv.LoadFromRDB(dump)
			} else {
				fmt.Println("RDB load error:", err)
			}
		}
	}

	kv.Info.flags = flags
}
