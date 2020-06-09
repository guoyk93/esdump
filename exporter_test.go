package esexporter

import (
	"context"
	"github.com/guoyk93/logutil"
	"github.com/olivere/elastic"
	"log"
	"os"
	"strconv"
	"testing"
)

func TestExporter_Do(t *testing.T) {
	max, _ := strconv.ParseInt(os.Getenv("ES_MAX"), 10, 64)
	batch, _ := strconv.ParseInt(os.Getenv("ES_BATCH"), 10, 64)
	var totalCount, totalSize, maxId int64

	prg := logutil.NewProgress(logutil.LoggerFunc(log.Printf), "test")

	handler := DocumentHandler(func(buf []byte, id int64, total int64) error {
		if max > 0 && id >= max {
			return ErrUserCancelled
		}
		log.Printf("%s", buf)
		prg.SetTotal(total)
		prg.SetCount(id)
		maxId = id
		totalCount = total
		totalSize += int64(len(buf))
		return nil
	})

	client, err := elastic.NewClient(elastic.SetURL(os.Getenv("ES_URL")))
	if err != nil {
		t.Fatal(err)
	}

	e := New(Options{
		Client:      client,
		Index:       os.Getenv("ES_INDEX"),
		Type:        os.Getenv("ES_TYPE"),
		Query:       elastic.NewTermQuery("project", os.Getenv("ES_PROJECT")),
		Size:        batch,
		DebugLogger: log.Printf,
	}, handler)

	if err := e.Do(context.Background()); err != nil {
		t.Fatal(err)
	}

	log.Printf("max id: %d", maxId)
	log.Printf("total count: %d", totalCount)
	log.Printf("total raw size: %dmb", totalSize/1024/1024)
}
