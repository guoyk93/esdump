package esexporter

import (
	"context"
	"errors"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/olivere/elastic"
	"io"
	"net/http"
	"net/url"
)

var (
	ErrUserCancelled = errors.New("user cancelled")
)

type SourceHandler func(buf []byte, id int64, total int64) error

type Options struct {
	Index         string
	Type          string
	Query         elastic.Query
	Scroll        string
	BatchByteSize int64
	NoMappingType bool
}

type Exporter interface {
	Do(ctx context.Context) error
}

type exporter struct {
	Options

	client  *elastic.Client
	handler SourceHandler

	scrollID string

	size   int64
	cursor int64
}

func (e *exporter) deleteScrollID() (err error) {
	scrollID := e.scrollID
	if scrollID == "" {
		return
	}
	if _, err = e.client.PerformRequest(context.Background(), elastic.PerformRequestOptions{
		Method: http.MethodDelete,
		Path:   "/_search/scroll",
		Body: map[string]interface{}{
			"scroll_id": scrollID,
		},
	}); err != nil {
		return
	}
	return
}

func (e *exporter) buildSearchPath() string {
	if e.NoMappingType {
		return "/" + e.Index + "/_search"
	} else {
		return "/" + e.Index + "/" + e.Type + "/_search"
	}
}

func (e *exporter) buildSearchBody(size interface{}) (b map[string]interface{}, err error) {
	b = map[string]interface{}{
		"size": size,
		// optimization, see https://www.elastic.co/guide/en/elasticsearch/reference/6.3/search-request-scroll.html
		"sort": []string{"_doc"},
	}
	if e.Query != nil {
		if b["query"], err = e.Query.Source(); err != nil {
			return
		}
	}
	return
}

func (e *exporter) estimateBatchSize(ctx context.Context) (err error) {
	const Sample = 512
	var body interface{}
	if body, err = e.buildSearchBody(Sample); err != nil {
		return
	}
	var res *elastic.Response
	if res, err = e.client.PerformRequest(ctx, elastic.PerformRequestOptions{
		Method: http.MethodPost,
		Path:   e.buildSearchPath(),
		Body:   body,
	}); err != nil {
		return
	}
	if len(res.Body) == 0 {
		err = errors.New("failed to estimate batch size: empty response")
		return
	}
	estByteSize := len(res.Body) / Sample
	if estByteSize == 0 {
		estByteSize = 512
	}
	e.size = e.BatchByteSize / int64(estByteSize)
	if e.size < 10 {
		e.size = 10
	} else if e.size > 10000 {
		e.size = 10000
	}
	return
}

func (e *exporter) do(ctx context.Context) (err error) {
	var res *elastic.Response
	if e.scrollID == "" {
		var body interface{}
		if body, err = e.buildSearchBody(e.size); err != nil {
			return
		}
		if res, err = e.client.PerformRequest(ctx, elastic.PerformRequestOptions{
			Method: http.MethodPost,
			Path:   e.buildSearchPath(),
			Params: url.Values{"scroll": []string{e.Scroll}},
			Body:   body,
		}); err != nil {
			return
		}
	} else {
		if res, err = e.client.PerformRequest(ctx, elastic.PerformRequestOptions{
			Method: http.MethodPost,
			Path:   "/_search/scroll",
			Body: map[string]interface{}{
				"scroll":    e.Scroll,
				"scroll_id": e.scrollID,
			},
		}); err != nil {
			return
		}
	}

	if res.StatusCode != http.StatusOK {
		err = fmt.Errorf("http request failed: %d: %s", res.StatusCode, res.Body)
		return
	}

	buf := res.Body

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
	var total int64
	if total, err = jsonparser.GetInt(buf, "hits", "total"); err != nil {
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
	var itCalled bool
	_, _ = jsonparser.ArrayEach(hitsBuf, func(value []byte, dataType jsonparser.ValueType, offset int, docErr error) {
		itCalled = true
		if itErr != nil {
			return
		}
		if docErr != nil {
			itErr = docErr
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
		if itErr = e.handler(srcBuf, e.cursor, total); itErr != nil {
			return
		}
		e.cursor = e.cursor + 1
	})

	if itErr != nil {
		err = itErr
		return
	}

	if !itCalled {
		err = io.EOF
		return
	}

	return
}

func (e *exporter) Do(ctx context.Context) (err error) {
	defer e.deleteScrollID()
	if err = e.estimateBatchSize(ctx); err != nil {
		return
	}
	for {
		if err = e.do(ctx); err != nil {
			if err == ErrUserCancelled || err == io.EOF {
				err = nil
			}
			return
		}
	}
}

func New(client *elastic.Client, opts Options, handler SourceHandler) Exporter {
	if opts.Type == "" {
		opts.Type = "_doc"
	}
	if opts.Scroll == "" {
		opts.Scroll = "1m"
	}
	if opts.BatchByteSize <= 0 {
		opts.BatchByteSize = 10 * 1024 * 1024
	}
	if handler == nil {
		handler = func(buf []byte, idx int64, total int64) error { return nil }
	}
	return &exporter{
		Options: opts,
		client:  client,
		handler: handler,
	}
}
