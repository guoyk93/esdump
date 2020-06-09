# esexporter

dump Elasticsearch documents with streaming JSON decoder, avoid decoding the whole response

## Intro (CN)

近期尝试使用 olivere/elastic 库从 Elasticsearch 集群上导出十多T的数据，发现 Scroll 的速度很慢。

跑了下 CPU Profile 发现，是 olivere/elastic 在请求结果 JSON 解析阶段消耗的时间太多了。

我只需要获取 `hits.hits` 中的 `source` 字段，结构化解析不仅慢，还非常消耗内存，因此我打造了这个库。

## Usage

```go
import (
 "github.com/guoyk93/esexporter"
)


err := esexporter.New(exexporter.Options{
    // ...
}, func (src []byte, id int64, total int64) error {
    return nil
}).Do(context.Background())
```

## Credits

Guo Y.K., MIT License
