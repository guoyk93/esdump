package esexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/buger/jsonparser"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
)

var (
	ErrUserCancelled = errors.New("user cancelled")
)

type DocumentHandler func(buf []byte, idx int64, total int64) error

type Options struct {
	Host        string
	Index       string
	Type        string
	Scroll      string
	Query       interface{}
	Batch       int64
	DebugLogger func(layout string, items ...interface{})
}

type Exporter interface {
	Do(ctx context.Context) error
}

type exporter struct {
	Options
	client   *http.Client
	scrollID string
	count    int64
	total    int64
	handler  DocumentHandler
	bufPool  *sync.Pool
}

func (e *exporter) buildRequest(ctx context.Context, method string, uri string, body interface{}) (req *http.Request, err error) {
	e.DebugLogger("exporter#buildRequest(%s, %s)", method, uri)
	var br io.Reader
	if body != nil {
		var buf []byte
		if buf, err = json.Marshal(body); err != nil {
			return
		}
		br = bytes.NewReader(buf)
	}
	if req, err = http.NewRequestWithContext(ctx, method, uri, br); err != nil {
		return
	}
	if br != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return
}

func (e *exporter) buildFirstURL() string {
	q := &url.Values{}
	q.Set("scroll", e.Scroll)
	u := &url.URL{
		Scheme:   "http",
		Host:     e.Host,
		Path:     "/" + e.Index + "/" + e.Type + "/_search",
		RawQuery: q.Encode(),
	}
	return u.String()
}

func (e *exporter) buildFirstRequest(ctx context.Context) (req *http.Request, err error) {
	e.DebugLogger("exporter#buildFirstRequest()")
	if req, err = e.buildRequest(ctx, http.MethodPost, e.buildFirstURL(), map[string]interface{}{
		"size":  e.Batch,
		"query": e.Query,
		// optimization, see https://www.elastic.co/guide/en/elasticsearch/reference/6.3/search-request-scroll.html
		"sort": []string{"_doc"},
	}); err != nil {
		return
	}
	return
}

func (e *exporter) buildNextURL() string {
	u := &url.URL{
		Scheme: "http",
		Host:   e.Host,
		Path:   "/_search/scroll",
	}
	return u.String()
}

func (e *exporter) buildNextRequest(ctx context.Context) (req *http.Request, err error) {
	e.DebugLogger("exporter#buildNextRequest()")
	if req, err = e.buildRequest(ctx, http.MethodPost, e.buildFirstURL(), map[string]interface{}{
		"scroll":    e.Scroll,
		"scroll_id": e.scrollID,
	}); err != nil {
		return
	}
	return
}

func (e *exporter) doRequest(req *http.Request) (err error) {
	e.DebugLogger("exporter#doRequest(%s)", req.URL.String())
	var res *http.Response
	if res, err = e.client.Do(req); err != nil {
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(res.Body)
		err = fmt.Errorf("http request failed: %d: %s", res.StatusCode, body)
		_ = res.Body.Close()
		return
	}

	buffer := e.bufPool.Get().(*bytes.Buffer)
	defer e.bufPool.Put(buffer)
	defer buffer.Reset()

	if _, err = io.Copy(buffer, res.Body); err != nil {
		return
	}

	buf := buffer.Bytes()

	// update scroll_id
	if e.scrollID, err = jsonparser.GetString(buf, "_scroll_id"); err != nil {
		return
	}

	// check shards failed
	var shardsFailed int64
	if shardsFailed, err = jsonparser.GetInt(buf, "_shards", "failed"); err != nil {
		return
	}
	if shardsFailed != 0 {
		err = errors.New("_shards.failed != 0")
		return
	}

	// check total
	if e.total, err = jsonparser.GetInt(buf, "hits", "total"); err != nil {
		return
	}

	// find hits.hits
	var hitsBuf []byte
	var hitsType jsonparser.ValueType
	if hitsBuf, hitsType, _, err = jsonparser.Get(buf, "hits", "hits"); err != nil {
		return
	}
	if hitsType != jsonparser.Array {
		err = errors.New("hits.hits is not array")
		return
	}

	// iterate hits.hits
	var itErr error
	var itCount int64
	_, _ = jsonparser.ArrayEach(hitsBuf, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		itCount++
		if itErr != nil {
			return
		}
		srcBuf, srcType, _, srcErr := jsonparser.Get(value, "_source")
		if srcErr != nil {
			itErr = srcErr
			return
		}
		if srcType != jsonparser.Object {
			itErr = errors.New("missing _source in hits.hits")
			return
		}
		if itErr = e.handler(srcBuf, e.count, e.total); err != nil {
			return
		}
		atomic.AddInt64(&e.count, 1)
	})

	if itErr != nil {
		err = itErr
		return
	}

	if itCount == 0 {
		err = io.EOF
		return
	}

	return
}

func (e *exporter) Do(ctx context.Context) (err error) {
	e.DebugLogger("exporter#Do()")
	var req *http.Request
	if req, err = e.buildFirstRequest(ctx); err != nil {
		return
	}
	if err = e.doRequest(req); err != nil {
		if err == ErrUserCancelled || err == io.EOF {
			err = nil
		}
		return
	}
	for {
		if req, err = e.buildNextRequest(ctx); err != nil {
			return
		}
		if err = e.doRequest(req); err != nil {
			if err == ErrUserCancelled || err == io.EOF {
				err = nil
			}
			return
		}
	}
}

func New(opts Options, handler DocumentHandler) Exporter {
	if opts.Type == "" {
		opts.Type = "_doc"
	}
	if opts.Scroll == "" {
		opts.Scroll = "1m"
	}
	if opts.Batch == 0 {
		opts.Batch = 5000
	}
	if opts.DebugLogger == nil {
		opts.DebugLogger = func(layout string, items ...interface{}) {}
	}
	if handler == nil {
		handler = func(buf []byte, idx int64, total int64) error { return nil }
	}
	return &exporter{
		Options: opts,
		client:  &http.Client{},
		handler: handler,
		bufPool: &sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
	}
}
