package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	CRLF               string = "\r\n"
	PROXY_AGENT_HEADER string = "Proxy-agent: proxy-server v0.1"
	BAD_GATEWAY        string = strings.Join([]string{
		"HTTP/1.1 502 Bad Gateway",
		PROXY_AGENT_HEADER,
		"Content-Length: 11",
		"Connection: close",
		CRLF,
	}, CRLF) + "Bad Gateway"
)

func main() {
	proxy := Proxy{
		addr: "localhost:3000",
		backends: []string{
			"localhost:5000",
			"localhost:5001",
		},
		roundrobin: 0,
		cache:      NewCache(100, 20),
	}
	proxy.ListenAndServe()
}

type Proxy struct {
	addr       string
	backends   []string
	roundrobin int
	cache      Cache
}

func (p *Proxy) ListenAndServe() {
	log.Println("Starting proxy...")
	listener, err := net.Listen("tcp", p.addr)
	if err != nil {
		log.Println(err.Error())
	}
	log.Println("Listenning on", p.addr)
	p.serve(listener)
}

func (p *Proxy) serve(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err.Error())
		}
		clientAddr := conn.RemoteAddr().String()
		log.Println("Accept connection from", clientAddr)
		go p.handle(conn)
	}
}

func (p *Proxy) handle(client net.Conn) {
	backend := p.loadBalance()
	server, err := net.DialTimeout("tcp", backend, time.Second*5)
	if err != nil {
		log.Println(err.Error())
		client.Write([]byte(BAD_GATEWAY))
		return
	}

	req := NewHTTPParser("Request")
	res := NewHTTPParser("Response")
	hmap := map[*HTTPParser]*HTTPParser{req: res, res: req}
	var pipe = func(src net.Conn, dst net.Conn, parser *HTTPParser) {
		defer func() {
			err := src.Close()
			if err != nil {
				log.Println("Connection", src.RemoteAddr().String(), "closed")
			}
			err = dst.Close()
			if err != nil {
				log.Println("Connection", dst.RemoteAddr().String(), "closed")
			}
		}()
		buff := make([]byte, 1024)
		for {
			n, err := src.Read(buff)
			if err != nil {
				return
			}
			log.Println(n, "bytes received from", src.RemoteAddr().String())
			// Parse received data
			parser.Parse(string(buff[:n]))
			if parser.State != "Complete" {
				continue
			}
			// Add custom header
			raw := parser.BuildRaw()
			if parser.Type == "Response" {
				log.Println("Add response to cache", hmap[parser].Url)
				key := strings.Replace(hmap[parser].Url, "/", "_", -1)
				p.cache.Set(key, raw)
			} else {
				if parser.Method == "GET" {
					key := strings.Replace(parser.Url, "/", "_", -1)
					if entry, ok := p.cache.Get(key); ok {
						object := entry.(string)
						src.Write([]byte(object))
						log.Println(len(object), "bytes sent to", src.RemoteAddr().String())
						return
					}
				}
			}
			_, err = dst.Write([]byte(raw))
			if err != nil {
				return
			}
			log.Println(n, "bytes sent to", dst.RemoteAddr().String())
		}
	}
	go pipe(client, server, req)
	go pipe(server, client, res)
}

func (p *Proxy) loadBalance() string {
	p.roundrobin += 1
	if p.roundrobin >= len(p.backends) {
		p.roundrobin = 0
	}
	return p.backends[p.roundrobin]
}

type HTTPParser struct {
	Method  string
	Url     string
	Version string
	Status  int
	Reason  string
	Headers map[string]string
	Body    string
	Raw     string
	State   string
	Type    string
}

func NewHTTPParser(ptype string) *HTTPParser {
	return &HTTPParser{
		Type:    ptype,
		Headers: make(map[string]string),
		State:   "Initialize",
	}
}

func split(data string) (string, string) {
	pos := strings.Index(data, CRLF)
	if pos < 0 {
		return "", data
	} else {
		return data[:pos], data[pos+len(CRLF):]
	}
}

func contains(key string, list []string) bool {
	for _, el := range list {
		if el == key {
			return true
		}
	}
	return false
}
func (p *HTTPParser) Parse(data string) {
	p.Raw += data
	more := len(data) > 0
	for more {
		more, data = p.process(data)
	}
}

func (p *HTTPParser) process(data string) (bool, string) {
	if p.State != "Complete" {
		if contains(p.State, []string{"HeadersReceived", "ReceivingBody"}) {
			if length, ok := p.Headers["Content-Length"]; ok {
				p.State = "ReceivingBody"
				if p.Body == "" {
					_, data = split(data)
				}
				p.Body += data
				if cl, _ := strconv.Atoi(length); len(p.Body) >= cl {
					p.State = "BodyReceived"
					p.State = "Complete"
					return false, ""
				}
			} else {
				p.State = "Complete"
				return false, ""
			}
		}
	}
	line, data := split(data)
	if len(line) == 0 {
		return false, data
	}
	if p.State == "Initialize" {
		p.processLine(line)
	}
	if contains(p.State, []string{"LineReceived", "ReceivingHeaders"}) {
		p.processHeader(data)
	}
	return len(data) > 0, data
}

func (p *HTTPParser) processLine(line string) {
	result := strings.Split(line, " ")
	if p.Type == "Request" {
		p.Method = result[0]
		p.Url = result[1]
		p.Version = result[2]
	} else {
		p.Version = result[0]
		p.Status, _ = strconv.Atoi(result[1])
		p.Reason = result[2]
	}
	p.State = "LineReceived"
}

func (p *HTTPParser) processHeader(data string) {
	if data == CRLF || strings.HasPrefix(data, CRLF) {
		p.State = "HeadersReceived"
	} else if strings.TrimSpace(data) != "" {
		p.State = "ReceivingHeaders"
		header := strings.Split(data, CRLF)[0]
		headerSplit := strings.Split(header, ": ")
		k, v := headerSplit[0], headerSplit[1]
		p.Headers[k] = v
	}
}

func (p *HTTPParser) BuildRaw() string {
	var raw string
	if p.Type == "Request" {
		raw = fmt.Sprintf("%s %s %s", p.Method, p.Url, p.Version)
	} else if p.Type == "Response" {
		raw = fmt.Sprintf("%s %d %s", p.Version, p.Status, p.Reason)
	}
	raw += CRLF
	for k, v := range p.Headers {
		raw += strings.Join([]string{k, v}, ": ") + CRLF
	}
	raw += CRLF + p.Body
	return raw
}

type CacheEntry struct {
	Object interface{}
	Expire int64
}

type Cache struct {
	Size       int
	DefaultExp int
	Entries    map[string]CacheEntry
	mutex      sync.Mutex
}

func NewCache(size int, defaultExp int) Cache {
	return Cache{
		Size:       size,
		DefaultExp: defaultExp,
		Entries:    make(map[string]CacheEntry),
	}
}
func (c *Cache) Set(key string, object interface{}) {
	if len(c.Entries) >= c.Size {
		c.Purge()
	}
	duration := time.Second * time.Duration(c.DefaultExp)
	entry := CacheEntry{
		Object: object,
		Expire: time.Now().Add(duration).Unix(),
	}
	c.mutex.Lock()
	c.Entries[key] = entry
	c.mutex.Unlock()
}

func (c *Cache) Get(key string) (interface{}, bool) {
	if entry, ok := c.Entries[key]; ok {
		if entry.Expire > time.Now().Unix() {
			return entry.Object, true
		} else {
			c.mutex.Lock()
			delete(c.Entries, key)
			c.mutex.Unlock()
			return nil, false
		}
	}
	return nil, false
}

func (c *Cache) Purge() {
	var toDelete []string
	for key, entry := range c.Entries {
		if entry.Expire > time.Now().Unix() {
			toDelete = append(toDelete, key)
		}
	}
	for _, key := range toDelete {
		c.mutex.Lock()
		delete(c.Entries, key)
		c.mutex.Unlock()
	}
}

type Parser struct {
	Protocol             *Session
	buffer               string
	doneParsingFirstLine bool
	doneParsingHeaders   bool
	Complete             bool
	expectedBodyLength   int
}

func NewParser(protocol *Session) Parser {
	return Parser{Protocol: protocol}
}

func (p *Parser) flush() string {
	temp := p.buffer
	p.buffer = ""
	return temp
}

func (p *Parser) split(data string) (string, string) {
	pos := strings.Index(data, CRLF)
	if pos < 0 {
		return "", data
	} else {
		return data[:pos], data[pos+len(CRLF):]
	}
}

func (p *Parser) Parse(data string) {
	p.buffer += data
	more := len(data) > 0
	for more {
		data, more = p.process(data)
	}
}

func (p *Parser) process(data string) (string, bool) {
	if p.expectedBodyLength > 0 && p.doneParsingHeaders {
		p.expectedBodyLength -= len(data)
		p.Protocol.onBody(data)
		if p.expectedBodyLength == 0 {
			p.Complete = true
			p.Protocol.onComplete()
		}
		return data, false
	}
	line, data := split(data)
	if !p.doneParsingFirstLine {
		p.processFirstLine(line)
	} else if !p.doneParsingHeaders {
		p.processHeader(line)
	} else if p.expectedBodyLength > 0 {
		tmp := p.flush()
		p.expectedBodyLength -= len(tmp)
		p.Protocol.onBody(tmp)
	} else {
		p.Complete = true
		p.Protocol.onComplete()
	}
	return data, len(data) > 0

}

func (p *Parser) processFirstLine(line string) {
	p.Protocol.onFirstLine(line)
	p.doneParsingFirstLine = true
}

func (p *Parser) processHeader(line string) {
	if line != "" {
		sp := strings.Split(line, ": ")
		k, v := sp[0], sp[1]
		if strings.ToLower(k) == "content-length" {
			p.expectedBodyLength, _ = strconv.Atoi(v)
		}
		p.Protocol.onHeader(k, v)
	} else {
		p.doneParsingHeaders = true
	}
}

type Session struct {
	Method  string
	URL     string
	Version string
	Status  string
	Reason  string
	Headers map[string]string
	Body    string
}

func NewSession() Session {
	return Session{Headers: make(map[string]string)}
}

func (s *Session) onFirstLine(line string) {
	log.Println("First line received", line)
	sp := strings.Split(line, " ")
	if strings.HasPrefix(sp[0], "HTTP") {
		s.Version, s.Status, s.Reason = sp[0], sp[1], sp[2]
	} else {
		s.Method, s.URL, s.Version = sp[0], sp[1], sp[2]
	}
}
func (s *Session) onHeader(k, v string) {
	log.Println("Header received", k, v)
	s.Headers[k] = v
}
func (s *Session) onBody(body string) {
	log.Println("Body received", body)
	s.Body += body
}
func (s *Session) onComplete() {
	log.Println("Data completely received")
}
func (s *Session) GetRaw() (string, error) {
	var raw string
	if s.Method != "" && s.URL != "" && s.Version != "" {
		raw = fmt.Sprintf("%s %s %s", s.Method, s.URL, s.Version)
	} else if s.Version != "" && s.Status != "" && s.Reason != "" {
		raw = fmt.Sprintf("%s %s %s", s.Version, s.Status, s.Reason)
	} else {
		return "", errors.New("Invalid http message")
	}
	raw += CRLF
	for k, v := range s.Headers {
		raw += fmt.Sprintf("%s: %s%s", k, v, CRLF)
	}
	raw += CRLF + s.Body
	return raw, nil
}
