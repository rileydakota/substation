package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"cloud.google.com/go/storage"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"golang.org/x/sync/errgroup"

	"github.com/brexhq/substation/v2"
	"github.com/brexhq/substation/v2/message"

	"github.com/brexhq/substation/v2/internal/bufio"
	"github.com/brexhq/substation/v2/internal/channel"
	"github.com/brexhq/substation/v2/internal/log"
	"github.com/brexhq/substation/v2/internal/media"
)

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Message struct {
		Data        string            `json:"data"`
		Attributes  map[string]string `json:"attributes"`
		MessageID   string            `json:"messageId"`
		PublishTime string            `json:"publishTime"`
	} `json:"message"`
}

// StorageObjectData contains metadata about a Cloud Storage object.
// This matches the format sent by Cloud Storage notifications to Pub/Sub.
type StorageObjectData struct {
	Bucket string `json:"bucket"`
	Name   string `json:"name"`
	Size   string `json:"size"`
	// Additional fields can be added as needed
	ContentType             string `json:"contentType,omitempty"`
	TimeCreated             string `json:"timeCreated,omitempty"`
	Updated                 string `json:"updated,omitempty"`
	Generation              string `json:"generation,omitempty"`
	MetaGeneration          string `json:"metageneration,omitempty"`
	StorageClass            string `json:"storageClass,omitempty"`
	TimeStorageClassUpdated string `json:"timeStorageClassUpdated,omitempty"`
}

// processingState tracks the current state of object processing for graceful shutdown logging.
type processingState struct {
	mu     sync.RWMutex
	bucket string
	object string
	size   string
}

func (ps *processingState) set(bucket, object, size string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.bucket = bucket
	ps.object = object
	ps.size = size
}

func (ps *processingState) log() {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if ps.bucket != "" && ps.object != "" {
		log.WithField("bucket", ps.bucket).
			WithField("object", ps.object).
			WithField("size", ps.size).
			Info("Interrupted while processing GCS object due to SIGTERM")
	}
}

// nolint: gocognit // Ignore cognitive complexity.
func pubSubHandler(ctx context.Context, e cloudevents.Event) error {
	// Set up signal handling for graceful shutdown
	var state processingState
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Handle signals in a separate goroutine
	go func() {
		sig := <-sigChan
		log.WithField("signal", sig.String()).Info("Received shutdown signal")
		state.log()
		// Allow the context cancellation to handle cleanup
	}()
	defer signal.Stop(sigChan)

	// Retrieve and load configuration.
	conf, err := getConfig(ctx)
	if err != nil {
		return err
	}

	cfg := customConfig{}
	if err := json.NewDecoder(conf).Decode(&cfg); err != nil {
		return err
	}

	// Catches an edge case where a missing concurrency value
	// can deadlock the application.
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}

	sub, err := substation.New(ctx, cfg.Config)
	if err != nil {
		return err
	}

	ch := channel.New[*message.Message]()
	group, ctx := errgroup.WithContext(ctx)

	// Data transformation. Transforms are executed concurrently using a worker pool
	// managed by an errgroup. Each message is processed in a separate goroutine.
	group.Go(func() error {
		tfGroup, tfCtx := errgroup.WithContext(ctx)
		tfGroup.SetLimit(cfg.Concurrency)

		for message := range ch.Recv() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			msg := message
			tfGroup.Go(func() error {
				// Transformed messages are never returned to the caller because
				// invocation is asynchronous.
				if _, err := sub.Transform(tfCtx, msg); err != nil {
					return err
				}

				return nil
			})
		}

		if err := tfGroup.Wait(); err != nil {
			return err
		}

		// CTRL messages flush the pipeline. This must be done
		// after all messages have been processed.
		ctrl := message.New().AsControl()
		if _, err := sub.Transform(tfCtx, ctrl); err != nil {
			return err
		}

		return nil
	})

	// Data ingest
	group.Go(func() error {
		defer ch.Close()

		var pubsubMsg PubSubMessage
		if err := json.Unmarshal(e.Data(), &pubsubMsg); err != nil {
			return fmt.Errorf("failed to unmarshal PubSub message: %v", err)
		}

		// Decode the base64-encoded data
		data, err := base64.StdEncoding.DecodeString(pubsubMsg.Message.Data)
		if err != nil {
			return fmt.Errorf("failed to decode message data: %v", err)
		}

		// Try to parse as Storage notification
		var storageObj StorageObjectData
		if err := json.Unmarshal(data, &storageObj); err != nil {
			return fmt.Errorf("failed to unmarshal storage object data: %v", err)
		}

		// Validate required fields
		if storageObj.Bucket == "" || storageObj.Name == "" {
			return fmt.Errorf("missing required fields: bucket=%q name=%q", storageObj.Bucket, storageObj.Name)
		}

		// Track current processing state for graceful shutdown
		state.set(storageObj.Bucket, storageObj.Name, storageObj.Size)

		log.WithField("bucket", storageObj.Bucket).WithField("object", storageObj.Name).Info("Processing GCS object.")

		// Create a storage client
		client, err := storage.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("storage.NewClient: %v", err)
		}
		defer client.Close()

		reader, err := client.Bucket(storageObj.Bucket).Object(storageObj.Name).NewReader(ctx)
		if err != nil {
			// Check if object doesn't exist and exit gracefully
		if err == storage.ErrObjectNotExist {
			log.WithField("bucket", storageObj.Bucket).
				WithField("object", storageObj.Name).
				Info("Object does not exist, skipping")
			return nil
		}
		return fmt.Errorf("Object(%q).NewReader: %v", storageObj.Name, err)
		}
		defer reader.Close()

		dst, err := os.CreateTemp("", "substation")
		if err != nil {
			return err
		}
		defer os.Remove(dst.Name())
		defer dst.Close()

		if _, err := io.Copy(dst, reader); err != nil {
			return fmt.Errorf("io.Copy: %w", err)
		}

		log.WithField("bucket", storageObj.Bucket).WithField("object", storageObj.Name).Info("Retrieved GCS object successfully.")

		// Determines if the file should be treated as text.
		// Text files are decompressed by the bufio package
		// (if necessary) and each line is sent as a separate
		// message. All other files are sent as a single message.
		mediaType, err := media.File(dst)
		if err != nil {
			return err
		}

		if _, err := dst.Seek(0, 0); err != nil {
			return err
		}

		// Create metadata that includes both storage object data and PubSub attributes
		metadataMap := map[string]interface{}{
			"bucket":      storageObj.Bucket,
			"name":        storageObj.Name,
			"size":        storageObj.Size,
			"messageId":   pubsubMsg.Message.MessageID,
			"publishTime": pubsubMsg.Message.PublishTime,
		}

		// Add optional fields if present
		if storageObj.ContentType != "" {
			metadataMap["contentType"] = storageObj.ContentType
		}
		if storageObj.TimeCreated != "" {
			metadataMap["timeCreated"] = storageObj.TimeCreated
		}

		// Add PubSub attributes if present
		if len(pubsubMsg.Message.Attributes) > 0 {
			metadataMap["attributes"] = pubsubMsg.Message.Attributes
		}

		metadata, err := json.Marshal(metadataMap)
		if err != nil {
			return err
		}

		// Unsupported media types are sent as binary data.
		if len(bufio.MediaTypes) > 0 && !contains(bufio.MediaTypes, mediaType) {
			r, err := io.ReadAll(dst)
			if err != nil {
				return err
			}

			msg := message.New().SetData(r).SetMetadata(metadata)
			ch.Send(msg)

			log.WithField("bucket", storageObj.Bucket).WithField("object", storageObj.Name).Info("Finished processing GCS object.")

			// Clear state after successful processing
			state.set("", "", "")

			return nil
		}

		scanner := bufio.NewScanner()
		defer scanner.Close()

		if err := scanner.ReadFile(dst); err != nil {
			return err
		}

		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			b := []byte(scanner.Text())
			msg := message.New().SetData(b).SetMetadata(metadata)

			ch.Send(msg)
		}

		if err := scanner.Err(); err != nil {
			return err
		}

		log.WithField("bucket", storageObj.Bucket).WithField("object", storageObj.Name).Info("Finished processing GCS object.")

		// Clear state after successful processing
		state.set("", "", "")

		return nil
	})

	// Wait for all goroutines to complete. This includes the goroutines that are
	// executing the transform functions.
	if err := group.Wait(); err != nil {
		return err
	}

	return nil
}

// contains checks if a slice contains a value.
func contains(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}
