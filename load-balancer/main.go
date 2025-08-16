package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Backend struct {
	u        *url.URL
	host     string
	client   *http.Client
	inflight int64
	alive    atomic.Bool // opcional (sem health actively)
}

var (
	backends []*Backend
	rr       uint64

	// headers hop-by-hop que não devem ser repassados
	hopHeaders = map[string]struct{}{
		"Connection":          {},
		"Proxy-Connection":    {},
		"Keep-Alive":          {},
		"Proxy-Authenticate":  {},
		"Proxy-Authorization": {},
		"Te":                  {}, // canonicalizado como "Te" em Go
		"Trailer":             {},
		"Transfer-Encoding":   {},
		"Upgrade":             {},
	}

	bufPool = sync.Pool{
		New: func() any { b := make([]byte, 32<<10); return &b }, // 32 KiB
	}
)

func mustEnvBackends() []string {
	raw := strings.TrimSpace(os.Getenv("BACKENDS"))
	if raw == "" {
		fmt.Fprintln(os.Stderr, "BACKENDS vazio. Ex: BACKENDS=http://backend1:8080,http://backend2:8080")
		os.Exit(2)
	}
	parts := strings.Split(raw, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		fmt.Fprintln(os.Stderr, "Nenhum backend válido em BACKENDS")
		os.Exit(2)
	}
	return out
}

func newBackend(raw string) *Backend {
	u, err := url.Parse(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "URL inválida: %s: %v\n", raw, err)
		os.Exit(2)
	}
	// Transporte super simples, HTTP/1.1, sem auto-decompress, com pool "quente".
	tr := &http.Transport{
		Proxy:                 nil,
		ForceAttemptHTTP2:     false, // HTTP/1.1 tende a ter p99 melhor em LAN curta sob pouca multiplexação
		DisableCompression:    true,  // não descomprimir no client; repassar bytes crus
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   1024,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 2 * time.Second, // curto e previsível
		ExpectContinueTimeout: 0,

		DialContext: (&net.Dialer{
			Timeout:   250 * time.Millisecond,
			KeepAlive: 30 * time.Second,
			Control: func(network, address string, c syscall.RawConn) error {
				var serr error
				if err := c.Control(func(fd uintptr) {
					// Força TCP_NODELAY (latência menor para pacotes pequenos)
					if e := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1); e != nil {
						serr = e
						return
					}
				}); err != nil {
					return err
				}
				return serr
			},
		}).DialContext,
	}

	return &Backend{
		u:      u,
		host:   u.Host,
		client: &http.Client{Transport: tr},
	}
}

func initBackends() {
	addrs := mustEnvBackends()
	backends = make([]*Backend, 0, len(addrs))
	for _, a := range addrs {
		b := newBackend(a)
		b.alive.Store(true)
		backends = append(backends, b)
	}
}

func chooseBackendP2C() *Backend {
	n := len(backends)
	if n == 0 {
		return nil
	}
	if n == 1 {
		return backends[0]
	}
	// 1° candidato: round-robin rápido
	i := int(atomic.AddUint64(&rr, 1)-1) % n
	// 2° candidato: outro índice (rand simples; 600 RPS -> lock do math/rand não vira gargalo)
	j := rand.Intn(n - 1)
	if j >= i {
		j++
	}
	a := backends[i]
	b := backends[j]

	// Prefira vivos
	if !a.alive.Load() && b.alive.Load() {
		return b
	}
	if !b.alive.Load() && a.alive.Load() {
		return a
	}
	// Menor carga in-flight vence (menos fila -> melhor p99)
	if atomic.LoadInt64(&a.inflight) <= atomic.LoadInt64(&b.inflight) {
		return a
	}
	return b
}

func singleJoiningSlash(a, b string) string {
	as := strings.HasSuffix(a, "/")
	bs := strings.HasPrefix(b, "/")
	switch {
	case as && bs:
		return a + b[1:]
	case !as && !bs:
		return a + "/" + b
	default:
		return a + b
	}
}

func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		if _, hop := hopHeaders[k]; hop {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func removeHopHeaders(h http.Header) {
	for k := range hopHeaders {
		h.Del(k)
	}
	// Se "Connection" listar cabeçalhos adicionais, remova-os também (RFC 7230).
	if cv := h.Get("Connection"); cv != "" {
		for _, f := range strings.Split(cv, ",") {
			if f = strings.TrimSpace(f); f != "" {
				h.Del(f)
			}
		}
	}
}

func proxy(w http.ResponseWriter, r *http.Request) {
	b := chooseBackendP2C()
	if b == nil {
		http.Error(w, "no backends", http.StatusBadGateway)
		return
	}

	atomic.AddInt64(&b.inflight, 1)
	defer atomic.AddInt64(&b.inflight, -1)

	// Deadline curto por request (protege p99)
	ctx, cancel := context.WithTimeout(r.Context(), 1200*time.Millisecond)
	defer cancel()

	outURL := *b.u
	outURL.Path = singleJoiningSlash(b.u.Path, r.URL.Path)
	outURL.RawQuery = r.URL.RawQuery

	// corpo é stream: não leia, só repasse
	outReq, err := http.NewRequestWithContext(ctx, r.Method, outURL.String(), r.Body)
	if err != nil {
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}

	// Cabeçalhos
	removeHopHeaders(r.Header)
	outReq.Header = r.Header.Clone()
	// X-Forwarded-For / Proto / Host
	if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil && ip != "" {
		prior := r.Header.Get("X-Forwarded-For")
		if prior != "" {
			outReq.Header.Set("X-Forwarded-For", prior+", "+ip)
		} else {
			outReq.Header.Set("X-Forwarded-For", ip)
		}
	}
	if r.TLS != nil {
		outReq.Header.Set("X-Forwarded-Proto", "https")
	} else {
		outReq.Header.Set("X-Forwarded-Proto", "http")
	}
	outReq.Header.Set("X-Forwarded-Host", r.Host)
	// Para evitar problemas de vhost no backend, use o host do backend:
	outReq.Host = b.host

	resp, err := b.client.Do(outReq)
	if err != nil {
		var nerr net.Error
		if errors.As(err, &nerr) && nerr.Timeout() {
			http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
		} else {
			http.Error(w, "bad gateway", http.StatusBadGateway)
		}
		return
	}
	defer resp.Body.Close()

	removeHopHeaders(resp.Header)
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	bufPtr := bufPool.Get().(*[]byte)
	_, _ = io.CopyBuffer(w, resp.Body, *bufPtr)
	bufPool.Put(bufPtr)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	initBackends()

	srv := &http.Server{
		Addr:              ":9999",
		Handler:           http.HandlerFunc(proxy),
		ReadHeaderTimeout: 1 * time.Second, // protege contra slowloris
		IdleTimeout:       60 * time.Second,
		// Sem WriteTimeout: streaming fim-a-fim; ajuste se precisar truncar respostas lentas
	}

	// Servidor HTTP simples.
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, "server error:", err)
		os.Exit(1)
	}
}
