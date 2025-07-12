package rdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
)

// DumpParser handles reading and interpreting a Redis RDB dump.
type DumpParser struct {
	reader    *bufio.Reader
	version   int
	metadata  map[string]string
	Databases []DatabaseSection
}

// DatabaseSection represents one logical Redis database from the dump.
type DatabaseSection struct {
	ID           int
	TotalEntries int
	TTL          uint64
	KeyValues    map[string]string
	TTLRecords   []TTLRecord
}

type TTLRecord struct {
	Key      string
	Value    string
	ExpireAt uint64
}

func NewParserFromBytes(data []byte) (*DumpParser, error) {
	tmp := "dump.rdb"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return nil, err
	}
	return NewParser(tmp)
}

func NewParser(path string) (*DumpParser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &DumpParser{
		reader:    bufio.NewReader(f),
		metadata:  make(map[string]string),
		Databases: make([]DatabaseSection, 0),
	}, nil
}

// readLength decodes the length or integer encoding prefix.
func (p *DumpParser) readLength() (length int, integer bool, err error) {
	first, err := p.reader.ReadByte()
	if err != nil {
		return
	}
	mode := first >> 6
	switch mode {
	case 0: // 6-bit length
		length = int(first)
	case 1: // 14-bit length
		second, err := p.reader.ReadByte()
		if err != nil {
			return 0, false, err
		}
		length = int((uint16(first&0x3F) << 8) | uint16(second))
	case 3: // integer encoding
		integer = true
		switch first & 0x3F {
		case 0:
			length = 1
		case 1:
			length = 2
		case 2:
			length = 4
		default:
			err = fmt.Errorf("unsupported integer code: %d", first&0x3F)
			return
		}
	default:
		err = fmt.Errorf("invalid length prefix: %02x", first)
	}
	return
}

// readString reads a raw string or integer as string from the dump.
func (p *DumpParser) readString() (string, error) {
	length, isInt, err := p.readLength()
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(p.reader, buf); err != nil {
		return "", err
	}
	if isInt {
		val, _ := binary.Varint(buf)
		return strconv.FormatInt(val, 10), nil
	}
	return string(buf), nil
}

// readMetadata extracts an AUX field if present.
func (p *DumpParser) readMetadata() error {
	b, err := p.reader.ReadByte()
	if err != nil {
		return err
	}
	if b != 0xFA {
		p.reader.UnreadByte()
		return fmt.Errorf("no metadata marker")
	}
	key, err := p.readString()
	if err != nil {
		return err
	}
	val, err := p.readString()
	if err != nil {
		return err
	}
	p.metadata[key] = val
	return nil
}

// readDatabase reads one SELECTDB block and its entries.
func (p *DumpParser) readDatabase() error {
	b, err := p.reader.ReadByte()
	if err != nil {
		return err
	}
	if b != 0xFE {
		p.reader.UnreadByte()
		return fmt.Errorf("no DB marker")
	}
	idx, _, err := p.readLength()
	if err != nil {
		return err
	}
	section := DatabaseSection{
		ID:         idx,
		KeyValues:  make(map[string]string),
		TTLRecords: []TTLRecord{},
	}
	p.Databases = append(p.Databases, section)

	// skip DB count flag
	if _, err := p.reader.ReadByte(); err != nil {
		return err
	}
	count, _, err := p.readLength()
	if err != nil {
		return err
	}
	ttlCount, _, err := p.readLength()
	if err != nil {
		return err
	}
	p.Databases[idx].TotalEntries = count
	p.Databases[idx].TTL = uint64(ttlCount)

	// load all key-value entries
	for {
		var expireAt uint64
		hasTTL := false
		b, err := p.reader.ReadByte()
		if err != nil {
			return err
		}
		if b == 0xFC {
			hasTTL = true
			buf := make([]byte, 8)
			if _, err := io.ReadFull(p.reader, buf); err != nil {
				return err
			}
			expireAt = binary.LittleEndian.Uint64(buf)
		} else {
			p.reader.UnreadByte()
		}

		kv, err := p.readKeyValue()
		if err != nil {
			if err.Error() == "not keyvalue" {
				break
			}
			return err
		}
		if hasTTL {
			p.Databases[idx].TTLRecords = append(p.Databases[idx].TTLRecords, TTLRecord{kv[0], kv[1], expireAt})
		} else {
			p.Databases[idx].KeyValues[kv[0]] = kv[1]
		}
	}
	return nil
}

// readKeyValue reads a single key-value pair (zero prefix).
func (p *DumpParser) readKeyValue() ([]string, error) {
	b, err := p.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if b != 0 {
		p.reader.UnreadByte()
		return nil, fmt.Errorf("not keyvalue")
	}
	key, err := p.readString()
	if err != nil {
		return nil, err
	}
	val, err := p.readString()
	if err != nil {
		return nil, err
	}
	return []string{key, val}, nil
}

// Parse processes the entire RDB stream.
func (p *DumpParser) Parse() error {
	head := make([]byte, 9)
	if _, err := io.ReadFull(p.reader, head); err != nil {
		return err
	}
	if string(head[:5]) != "REDIS" {
		return fmt.Errorf("invalid header")
	}
	ver, err := strconv.Atoi(string(head[5:9]))
	if err != nil {
		return err
	}
	p.version = ver

	// read all metadata blocks
	for {
		if err := p.readMetadata(); err != nil {
			break
		}
	}

	// read all DB sections
	for {
		if err := p.readDatabase(); err != nil {
			break
		}
	}

	// expect closing marker 0xFF
	if b, err := p.reader.ReadByte(); err != nil {
		return err
	} else if b != 0xFF {
		return fmt.Errorf("unexpected end byte: %02x", b)
	}
	return nil
}
