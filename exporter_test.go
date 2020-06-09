package esexporter

import (
	"context"
	"github.com/guoyk93/logutil"
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
		prg.SetTotal(total)
		prg.SetCount(id)
		maxId = id
		totalCount = total
		totalSize += int64(len(buf))
		return nil
	})

	e := New(Options{
		Host:  os.Getenv("ES_HOST"),
		Index: os.Getenv("ES_INDEX"),
		Type:  os.Getenv("ES_TYPE"),
		Query: map[string]interface{}{
			"term": map[string]interface{}{
				"project": os.Getenv("ES_PROJECT"),
			},
		},
		Batch:       batch,
		DebugLogger: log.Printf,
	}, handler)

	if err := e.Do(context.Background()); err != nil {
		t.Fatal(err)
	}

	log.Printf("max id: %d", maxId)
	log.Printf("total count: %d", totalCount)
	log.Printf("total raw size: %dmb", totalSize/1024/1024)
}
