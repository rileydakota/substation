package transform

import (
	"bytes"
	"context"
	"testing"

	"github.com/brexhq/substation/v2/config"
	"github.com/brexhq/substation/v2/message"
	"github.com/parquet-go/parquet-go"
)

var _ Transformer = &formatToParquet{}

var formatToParquetTests = []struct {
	name     string
	cfg      config.Config
	test     []byte
	expected bool // true if output should be valid parquet
	err      error
}{
	{
		"array of objects",
		config.Config{},
		[]byte(`[{"name":"alice","age":30},{"name":"bob","age":25}]`),
		true,
		nil,
	},
	{
		"array with null values",
		config.Config{},
		[]byte(`[{"name":"alice","age":null},{"name":"bob","age":25}]`),
		true,
		nil,
	},
	{
		"array with nested objects",
		config.Config{},
		[]byte(`[{"name":"alice","meta":{"role":"admin"}},{"name":"bob","meta":{"role":"user"}}]`),
		true,
		nil,
	},
	{
		"array with mixed types",
		config.Config{},
		[]byte(`[{"value":123},{"value":"string"},{"value":true}]`),
		true,
		nil,
	},
	{
		"single object - not converted",
		config.Config{},
		[]byte(`{"name":"alice"}`),
		false, // should pass through unchanged
		nil,
	},
	{
		"string - not converted",
		config.Config{},
		[]byte(`"hello"`),
		false, // should pass through unchanged
		nil,
	},
	{
		"empty array",
		config.Config{},
		[]byte(`[]`),
		true,
		nil,
	},
	{
		"array with sparse fields",
		config.Config{},
		[]byte(`[{"a":"1"},{"b":"2"},{"a":"3","b":"4"}]`),
		true,
		nil,
	},
}

func TestFormatToParquet(t *testing.T) {
	ctx := context.TODO()
	for _, test := range formatToParquetTests {
		t.Run(test.name, func(t *testing.T) {
			msg := message.New().SetData(test.test)

			tf, err := newFormatToParquet(ctx, test.cfg)
			if err != nil {
				t.Fatal(err)
			}

			result, err := tf.Transform(ctx, msg)
			if err != nil {
				if test.err == nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}

			if len(result) != 1 {
				t.Fatalf("expected 1 message, got %d", len(result))
			}

			data := result[0].Data()

			if test.expected {
				// Verify it's valid parquet by attempting to read it
				reader := bytes.NewReader(data)
				file, err := parquet.OpenFile(reader, int64(len(data)))
				if err != nil {
					t.Errorf("output is not valid parquet: %v", err)
					return
				}

				// Verify we can read rows
				rows := file.NumRows()
				if rows < 0 {
					t.Errorf("invalid number of rows: %d", rows)
				}
			} else {
				// Should be unchanged (pass-through)
				if !bytes.Equal(data, test.test) {
					t.Errorf("expected data to be unchanged, got different output")
				}
			}
		})
	}
}

func TestFormatToParquetControlMessage(t *testing.T) {
	ctx := context.TODO()
	tf, err := newFormatToParquet(ctx, config.Config{})
	if err != nil {
		t.Fatal(err)
	}

	// Control messages should pass through unchanged
	msg := message.New().AsControl()
	result, err := tf.Transform(ctx, msg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 message, got %d", len(result))
	}

	if !result[0].IsControl() {
		t.Error("expected control message to pass through")
	}
}

func TestFormatToParquetInvalidJSON(t *testing.T) {
	ctx := context.TODO()
	tf, err := newFormatToParquet(ctx, config.Config{})
	if err != nil {
		t.Fatal(err)
	}

	// Invalid JSON that looks like an array should error
	msg := message.New().SetData([]byte(`[{"invalid`))
	_, err = tf.Transform(ctx, msg)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestFormatToParquetString(t *testing.T) {
	ctx := context.TODO()
	tf, err := newFormatToParquet(ctx, config.Config{
		Settings: map[string]interface{}{
			"id": "test_parquet",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	str := tf.String()
	if str != `{"id":"test_parquet"}` {
		t.Errorf("unexpected string: %s", str)
	}
}

func benchmarkFormatToParquet(b *testing.B, tf *formatToParquet, data []byte) {
	ctx := context.TODO()
	for i := 0; i < b.N; i++ {
		msg := message.New().SetData(data)
		_, _ = tf.Transform(ctx, msg)
	}
}

func BenchmarkFormatToParquet(b *testing.B) {
	for _, test := range formatToParquetTests {
		if !test.expected {
			continue // Skip non-parquet tests
		}
		tf, err := newFormatToParquet(context.TODO(), test.cfg)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(test.name,
			func(b *testing.B) {
				benchmarkFormatToParquet(b, tf, test.test)
			},
		)
	}
}

func FuzzTestFormatToParquet(f *testing.F) {
	testcases := [][]byte{
		[]byte(`[]`),
		[]byte(`[{"a":"b"}]`),
		[]byte(`[{"a":1},{"a":2}]`),
		[]byte(`[{"a":null}]`),
	}

	for _, tc := range testcases {
		f.Add(tc)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		ctx := context.TODO()
		msg := message.New().SetData(data)

		tf, err := newFormatToParquet(ctx, config.Config{})
		if err != nil {
			return
		}

		_, err = tf.Transform(ctx, msg)
		if err != nil {
			return
		}
	})
}
