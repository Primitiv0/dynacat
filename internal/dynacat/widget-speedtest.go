package dynacat

// LibreSpeed test logic in this file is adapted from librespeed/speedtest-cli
// https://github.com/librespeed/speedtest-cli (GNU LGPL v3.0).

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var speedtestWidgetTemplate = mustParseTemplate("speedtest.html", "widget-base.html")

const (
	speedtestDefaultListURL  = "https://librespeed.org/backend-servers/servers.php"
	speedtestDefaultDlPath   = "backend/garbage.php"
	speedtestDefaultUlPath   = "backend/empty.php"
	speedtestDefaultPingPath = "backend/empty.php"
	speedtestPingCount       = 10
	speedtestStagger         = 200 * time.Millisecond
	speedtestUploadChunk     = 1024 * 1024 // 1 MiB
)

type speedtestWidget struct {
	widgetBase    `yaml:",inline"`
	Frameless  bool          `yaml:"frameless"`
	Server     string        `yaml:"server"`
	Duration   durationField `yaml:"duration"`
	Concurrent int           `yaml:"concurrent"`

	mu       sync.Mutex
	running  bool
	started  bool
	result   *speedtestResult
	selected *speedtestServer
	client   *http.Client
}

type speedtestResult struct {
	DownloadMbps float64
	UploadMbps   float64
	PingMs       float64
	ServerName   string
}

type speedtestServer struct {
	Name    string `json:"name"`
	Server  string `json:"server"`
	DlURL   string `json:"dlURL"`
	UlURL   string `json:"ulURL"`
	PingURL string `json:"pingURL"`
}

func (widget *speedtestWidget) initialize() error {
	widget.withTitle("Speed Test").withCacheDuration(6 * time.Hour)
	widget.widgetBase.WIP = true

	if widget.UpdateInterval == nil {
		interval := updateIntervalField(6 * time.Hour)
		widget.UpdateInterval = &interval
	}

	if widget.Duration <= 0 {
		widget.Duration = durationField(15 * time.Second)
	}

	if widget.Concurrent <= 0 {
		widget.Concurrent = 3
	}

	widget.Server = strings.TrimRight(widget.Server, "/")

	widget.client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: widget.Concurrent + 2,
			MaxConnsPerHost:     widget.Concurrent + 2,
			Proxy:               http.ProxyFromEnvironment,
		},
	}

	return nil
}

// update launches the test in a detached goroutine and returns immediately, so a long
// (~35s) run never stalls the page update batch. Displayed fields change only when the
// test finishes, so partial numbers are never shown.
func (widget *speedtestWidget) update(context.Context) {
	widget.mu.Lock()
	if widget.running {
		widget.mu.Unlock()
		return
	}
	widget.running = true
	widget.started = true
	widget.ContentAvailable = true
	widget.mu.Unlock()

	widget.scheduleNextUpdate()

	go func() {
		timeout := 2*time.Duration(widget.Duration) + 45*time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		res, err := widget.runTest(ctx)

		widget.mu.Lock()
		if err == nil {
			widget.result = res
		}
		widget.running = false
		widget.mu.Unlock()

		if err != nil {
			widget.withNotice(fmt.Errorf("speed test failed: %w", err))
		} else {
			widget.withNotice(nil)
		}
	}()
}

func (widget *speedtestWidget) Render() template.HTML {
	return widget.renderTemplate(widget, speedtestWidgetTemplate)
}

// Template accessors (mutex-guarded: the background goroutine writes result concurrently).

func (widget *speedtestWidget) HasResult() bool {
	widget.mu.Lock()
	defer widget.mu.Unlock()
	return widget.result != nil
}

func (widget *speedtestWidget) IsTesting() bool {
	widget.mu.Lock()
	defer widget.mu.Unlock()
	return widget.started && widget.result == nil
}

func (widget *speedtestWidget) Download() string {
	return widget.formatMetric(func(r *speedtestResult) float64 { return r.DownloadMbps }, 1)
}

func (widget *speedtestWidget) Upload() string {
	return widget.formatMetric(func(r *speedtestResult) float64 { return r.UploadMbps }, 1)
}

func (widget *speedtestWidget) Ping() string {
	return widget.formatMetric(func(r *speedtestResult) float64 { return r.PingMs }, 0)
}

func (widget *speedtestWidget) ServerName() string {
	widget.mu.Lock()
	defer widget.mu.Unlock()
	if widget.result == nil {
		return ""
	}
	return widget.result.ServerName
}

func (widget *speedtestWidget) formatMetric(get func(*speedtestResult) float64, decimals int) string {
	widget.mu.Lock()
	defer widget.mu.Unlock()
	if widget.result == nil {
		return "-"
	}
	return strconv.FormatFloat(get(widget.result), 'f', decimals, 64)
}

// runTest selects a server (if needed), then measures ping, download and upload.

func (widget *speedtestWidget) runTest(ctx context.Context) (*speedtestResult, error) {
	srv, err := widget.resolveServer(ctx)
	if err != nil {
		return nil, err
	}

	ping, err := widget.measurePing(ctx, srv)
	if err != nil {
		widget.mu.Lock()
		widget.selected = nil // force re-selection next run
		widget.mu.Unlock()
		return nil, err
	}

	dl, err := widget.measureStream(ctx, srv, false)
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}

	ul, err := widget.measureStream(ctx, srv, true)
	if err != nil {
		return nil, fmt.Errorf("upload: %w", err)
	}

	return &speedtestResult{
		DownloadMbps: dl,
		UploadMbps:   ul,
		PingMs:       ping,
		ServerName:   srv.Name,
	}, nil
}

func (widget *speedtestWidget) resolveServer(ctx context.Context) (*speedtestServer, error) {
	if widget.Server != "" {
		return &speedtestServer{
			Name:    widget.Server,
			Server:  widget.Server,
			DlURL:   speedtestDefaultDlPath,
			UlURL:   speedtestDefaultUlPath,
			PingURL: speedtestDefaultPingPath,
		}, nil
	}

	widget.mu.Lock()
	cached := widget.selected
	widget.mu.Unlock()
	if cached != nil {
		return cached, nil
	}

	srv, err := widget.autoSelectServer(ctx)
	if err != nil {
		return nil, err
	}

	widget.mu.Lock()
	widget.selected = srv
	widget.mu.Unlock()
	return srv, nil
}

func (widget *speedtestWidget) autoSelectServer(ctx context.Context) (*speedtestServer, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, speedtestDefaultListURL, nil)
	if err != nil {
		return nil, err
	}

	servers, err := decodeJsonFromRequest[[]speedtestServer](widget.client, req)
	if err != nil {
		return nil, fmt.Errorf("fetching server list: %w", err)
	}
	if len(servers) == 0 {
		return nil, fmt.Errorf("server list is empty")
	}

	type pingResult struct {
		idx     int
		latency time.Duration
	}

	results := make(chan pingResult, len(servers))
	var wg sync.WaitGroup

	for i := range servers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			latency, err := widget.pingOnce(pingCtx, &servers[idx])
			if err != nil {
				results <- pingResult{idx: idx, latency: 0}
				return
			}
			results <- pingResult{idx: idx, latency: latency}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	best := -1
	var bestLatency time.Duration
	for r := range results {
		if r.latency <= 0 {
			continue
		}
		if best == -1 || r.latency < bestLatency {
			best = r.idx
			bestLatency = r.latency
		}
	}

	if best == -1 {
		return nil, fmt.Errorf("no reachable speed test server found")
	}

	srv := servers[best]
	return &srv, nil
}

func speedtestJoinURL(base, path string) string {
	if strings.HasPrefix(base, "//") {
		base = "https:" + base
	}
	base = strings.TrimRight(base, "/")
	path = strings.TrimLeft(path, "/")
	return base + "/" + path
}

func (widget *speedtestWidget) pingOnce(ctx context.Context, srv *speedtestServer) (time.Duration, error) {
	url := speedtestJoinURL(srv.Server, srv.PingURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "dynacat-speedtest")

	start := time.Now()
	resp, err := widget.client.Do(req)
	if err != nil {
		return 0, err
	}
	elapsed := time.Since(start)
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1024)) //nolint:errcheck
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return elapsed, nil
}

func (widget *speedtestWidget) measurePing(ctx context.Context, srv *speedtestServer) (float64, error) {
	var total time.Duration
	var count int

	for i := 0; i < speedtestPingCount; i++ {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		latency, err := widget.pingOnce(pingCtx, srv)
		cancel()
		if err != nil {
			if i == 0 {
				return 0, err
			}
			continue
		}
		if i == 0 {
			continue // discard first sample (handshake overhead)
		}
		total += latency
		count++
	}

	if count == 0 {
		return 0, fmt.Errorf("all pings failed")
	}

	return float64(total.Microseconds()) / float64(count) / 1000.0, nil
}

// measureStream runs Concurrent staggered streams for Duration and returns Mbps.
// upload=false drives GET garbage downloads, upload=true drives POST uploads.
func (widget *speedtestWidget) measureStream(ctx context.Context, srv *speedtestServer, upload bool) (float64, error) {
	streamCtx, cancel := context.WithTimeout(ctx, time.Duration(widget.Duration))
	defer cancel()

	var counter atomic.Int64
	var wg sync.WaitGroup

	var payload []byte
	if upload {
		payload = make([]byte, speedtestUploadChunk)
		if _, err := rand.Read(payload); err != nil {
			return 0, err
		}
	}

	start := time.Now()

	for i := 0; i < widget.Concurrent; i++ {
		select {
		case <-streamCtx.Done():
		case <-time.After(time.Duration(i) * speedtestStagger):
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for streamCtx.Err() == nil {
				if upload {
					widget.uploadOnce(streamCtx, srv, payload, &counter)
				} else {
					widget.downloadOnce(streamCtx, srv, &counter)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		return 0, fmt.Errorf("invalid elapsed time")
	}

	mbps := float64(counter.Load()) / elapsed / 125000.0
	return mbps, nil
}

func (widget *speedtestWidget) downloadOnce(ctx context.Context, srv *speedtestServer, counter *atomic.Int64) {
	url := speedtestJoinURL(srv.Server, srv.DlURL) + "?ckSize=100"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", "dynacat-speedtest")

	resp, err := widget.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	buf := make([]byte, 64*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			counter.Add(int64(n))
		}
		if err != nil {
			return
		}
	}
}

func (widget *speedtestWidget) uploadOnce(ctx context.Context, srv *speedtestServer, payload []byte, counter *atomic.Int64) {
	url := speedtestJoinURL(srv.Server, srv.UlURL)
	body := &speedtestCountingReader{reader: bytes.NewReader(payload), counter: counter}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return
	}
	req.ContentLength = int64(len(payload))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("User-Agent", "dynacat-speedtest")

	resp, err := widget.client.Do(req)
	if err != nil {
		return
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1024)) //nolint:errcheck
	resp.Body.Close()
}

// speedtestCountingReader counts bytes actually sent by the transport.
type speedtestCountingReader struct {
	reader  *bytes.Reader
	counter *atomic.Int64
}

func (r *speedtestCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.counter.Add(int64(n))
	}
	return n, err
}
