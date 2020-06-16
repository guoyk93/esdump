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
	Index  string
	Type   string
	Query  elastic.Query
	Scroll string
	Size   int64
}

type Exporter interface {
	Do(ctx context.Context) error
}

type exporter struct {
	Options

	client  *elastic.Client
	handler SourceHandler

	scrollID string

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

func (e *exporter) do(ctx context.Context) (err error) {
	var res *elastic.Response
	if e.scrollID == "" {
		var querySrc interface{}
		if e.Query != nil {
			if querySrc, err = e.Query.Source(); err != nil {
				return
			}
		}
		if res, err = e.client.PerformRequest(ctx, elastic.PerformRequestOptions{
			Method: http.MethodPost,
			Path:   "/" + e.Index + "/" + e.Type + "/_search",
			Params: url.Values{"scroll": []string{e.Scroll}},
			Body: map[string]interface{}{
				"size":  e.Size,
				"query": querySrc,
				// optimization, see https://www.elastic.co/guide/en/elasticsearch/reference/6.3/search-request-scroll.html
				"sort": []string{"_doc"},
			},
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
	if opts.Size == 0 {
		opts.Size = 5000
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
