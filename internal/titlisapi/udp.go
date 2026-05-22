package titlisapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

type Envelope struct {
	V        int             `json:"v"`
	T        string          `json:"t"`
	Ts       int64           `json:"ts"`
	TenantID int64           `json:"tenant_id,omitempty"`
	APIKey   string          `json:"api_key,omitempty"`
	Data     json.RawMessage `json:"data"`
}

type UDPClient interface {
	Send(ctx context.Context, eventType string, tenantID int64, data any) error
}

// HTTPEventClient sends prbot events to titlis-api via HTTP using X-Internal-Secret auth.
// This replaces the UDP transport which lacked authentication support.
type HTTPEventClient struct {
	baseURL string
	secret  string
	http    *http.Client
}

func NewHTTPEventClient(host string, port int, secret string) *HTTPEventClient {
	return &HTTPEventClient{
		baseURL: fmt.Sprintf("http://%s:%d", host, port),
		secret:  secret,
		http:    &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *HTTPEventClient) Send(ctx context.Context, eventType string, tenantID int64, data any) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}
	env := Envelope{
		V:        1,
		T:        eventType,
		Ts:       time.Now().UnixMilli(),
		TenantID: tenantID,
		Data:     payload,
	}
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/internal/prbot/events", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Secret", c.secret)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("titlis-api events: status %d", resp.StatusCode)
	}
	return nil
}

// RealUDPClient is kept for local/test environments where HTTP is not available.
type RealUDPClient struct {
	host string
	port int
}

func NewUDPClient(host string, port int) *RealUDPClient {
	return &RealUDPClient{host: host, port: port}
}

func (c *RealUDPClient) Send(_ context.Context, eventType string, tenantID int64, data any) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}
	env := Envelope{
		V:        1,
		T:        eventType,
		Ts:       time.Now().UnixMilli(),
		TenantID: tenantID,
		Data:     payload,
	}
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	addr := net.JoinHostPort(c.host, fmt.Sprintf("%d", c.port))
	conn, err := net.DialTimeout("udp", addr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Write(body)
	return err
}

type NoopEventClient struct{}

func NewNoopEventClient() *NoopEventClient { return &NoopEventClient{} }

func (NoopEventClient) Send(_ context.Context, _ string, _ int64, _ any) error { return nil }

type RecordedEvent struct {
	Type     string
	TenantID int64
	Data     any
	At       time.Time
}

type MemoryUDPClient struct {
	mu     sync.Mutex
	events []RecordedEvent
}

func NewMemoryUDPClient() *MemoryUDPClient {
	return &MemoryUDPClient{}
}

func (m *MemoryUDPClient) Send(_ context.Context, eventType string, tenantID int64, data any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, RecordedEvent{Type: eventType, TenantID: tenantID, Data: data, At: time.Now().UTC()})
	return nil
}

func (m *MemoryUDPClient) Events() []RecordedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]RecordedEvent(nil), m.events...)
}
