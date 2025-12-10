package respgo

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type RespParser struct {
	r *bufio.Reader
}

func NewParser(src io.Reader) *RespParser {
	return &RespParser{r: bufio.NewReader(src)}
}

func (p *RespParser) readLine() (string, error) {
	line, err := p.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	return line, nil
}

func (p *RespParser) readLength() (int, error) {
	s, err := p.readLine()
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid length %q: %w", s, err)
	}
	return n, nil
}

func EncodeRawArray(frames ...[]byte) []byte {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("*%d\r\n", len(frames)))
	for _, f := range frames {
		buf.Write(f)
	}
	return buf.Bytes()
}

func (p *RespParser) ParseBulk() ([]byte, error) {

	n, err := p.readLength()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}

	buf := make([]byte, n+2)
	if _, err := io.ReadFull(p.r, buf); err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (p *RespParser) ParseArray() ([]string, error) {
	countLine, err := p.readLine()
	if err != nil {
		return nil, err
	}
	cnt, err := strconv.Atoi(strings.TrimPrefix(countLine, "*"))
	if err != nil {
		return nil, fmt.Errorf("bad array count %q: %w ", countLine, err)
	}

	result := make([]string, cnt)
	for i := 0; i < cnt; i++ {
		prefix, err := p.r.ReadByte()
		if err != nil {
			return nil, err
		}
		if prefix != '$' {
			return nil, errors.New("expected bulk string in array")
		}
		data, err := p.ParseBulk()
		if err != nil {
			return nil, err
		}
		result[i] = string(data)
	}
	return result, nil
}

func (p *RespParser) ParseMessage() (interface{}, error) {
	b, err := p.r.Peek(1)
	if err != nil {
		return nil, err
	}
	switch b[0] {
	case '*':
		p.r.ReadByte()
		return p.ParseArray()
	case '$':
		p.r.ReadByte()
		return p.ParseBulk()
	case ':':
		p.r.ReadByte()
		line, err := p.readLine()
		if err != nil {
			return nil, err
		}
		return strconv.Atoi(line)
	default:
		line, err := p.readLine()
		if err != nil {
			return nil, err
		}
		return line, nil
	}
}

func EncodeBulkString(s string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

func EncodeArray(items []string) []byte {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(items)))
	for _, it := range items {
		sb.WriteString(string(EncodeBulkString(it)))
	}
	return []byte(sb.String())
}

func EncodeInteger(n int) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", n))
}
