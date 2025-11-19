package transform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/brexhq/substation/v2/config"
	"github.com/brexhq/substation/v2/message"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	iconfig "github.com/brexhq/substation/v2/internal/config"
)

type formatToParquetConfig struct {
	ID string `json:"id"`
}

func (c *formatToParquetConfig) Decode(in interface{}) error {
	return iconfig.Decode(in, c)
}

// inferStringSchema creates a parquet schema with all fields as optional strings.
func inferStringSchema(data []map[string]any) *parquet.Schema {
	group := make(parquet.Group)
	for _, record := range data {
		for key := range record {
			if _, exists := group[key]; !exists {
				group[key] = parquet.Optional(parquet.String())
			}
		}
	}
	return parquet.NewSchema("substation", group)
}

// convertToStrings converts all values in the data to strings.
func convertToStrings(data []map[string]any) ([]map[string]*string, error) {
	result := make([]map[string]*string, len(data))
	for i, record := range data {
		result[i] = make(map[string]*string)
		for key, value := range record {
			if value == nil {
				result[i][key] = nil
				continue
			}
			var str string
			switch v := value.(type) {
			case string:
				str = v
			default:
				// For non-strings (numbers, bools, objects, arrays), marshal to JSON
				b, err := json.Marshal(v)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal field %q: %v", key, err)
				}
				str = string(b)
			}
			result[i][key] = &str
		}
	}
	return result, nil
}

func newFormatToParquet(_ context.Context, cfg config.Config) (*formatToParquet, error) {
	conf := formatToParquetConfig{}
	if err := conf.Decode(cfg.Settings); err != nil {
		return nil, fmt.Errorf("transform format_to_parquet: %v", err)
	}

	if conf.ID == "" {
		conf.ID = "format_to_parquet"
	}

	tf := formatToParquet{
		conf: conf,
	}

	return &tf, nil
}

type formatToParquet struct {
	conf formatToParquetConfig
}

func (tf *formatToParquet) Transform(ctx context.Context, msg *message.Message) ([]*message.Message, error) {
	if msg.IsControl() {
		return []*message.Message{msg}, nil
	}

	// The message must be an array of objects to be converted to a Parquet file.
	if !msg.GetValue("@this").IsArray() {
		return []*message.Message{msg}, nil
	}

	var data []map[string]any
	if err := json.Unmarshal(msg.Data(), &data); err != nil {
		return nil, fmt.Errorf("transform %s: failed to parse JSON: %v", tf.conf.ID, err)
	}

	// Infer schema from data - all fields as optional strings
	schema := inferStringSchema(data)

	// Convert all values to strings
	stringData, err := convertToStrings(data)
	if err != nil {
		return nil, fmt.Errorf("transform %s: %v", tf.conf.ID, err)
	}

	var b bytes.Buffer
	if err := parquet.Write(&b, stringData, schema, parquet.Compression(&zstd.Codec{})); err != nil {
		return nil, fmt.Errorf("transform %s: %v", tf.conf.ID, err)
	}

	msg.SetData(b.Bytes())
	return []*message.Message{msg}, nil
}

func (tf *formatToParquet) String() string {
	b, _ := json.Marshal(tf.conf)
	return string(b)
}
