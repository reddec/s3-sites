// Package storage wraps an S3-compatible object store behind the read-only
// listing and fetching a site renderer needs.
package storage

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config describes the bucket backing storage and the credentials to reach it.
// An empty Endpoint selects the default AWS endpoint resolution for the region.
// Credentials are used exactly as configured and are never resolved from the
// environment; leaving both empty sends unsigned requests.
type Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	PathStyle       bool
	AccessKeyID     string
	SecretAccessKey string
}

// Storage is a read-only view of one bucket.
type Storage struct {
	s3     *s3.Client
	bucket string
}

// New scopes the store to cfg, the only source of credentials. A configured
// pair reaches the signer verbatim; an empty pair leaves requests unsigned, so
// the store's own policy decides whether they are served.
func New(ctx context.Context, cfg Config) (*Storage, error) {
	provider := aws.CredentialsProvider(aws.AnonymousCredentials{})
	if cfg.AccessKeyID != "" || cfg.SecretAccessKey != "" {
		provider = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     cfg.AccessKeyID,
				SecretAccessKey: cfg.SecretAccessKey,
				Source:          "config",
			}, nil
		})
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(provider),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	api := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.PathStyle
		if cfg.Endpoint != "" {
			o.BaseEndpoint = new(cfg.Endpoint)
		}
	})
	return &Storage{s3: api, bucket: cfg.Bucket}, nil
}

// List returns the directories directly under the bucket root, each without
// its trailing slash, in the order the store lists them. Objects sitting at
// the root are not directories and are skipped.
func (s *Storage) List(ctx context.Context) ([]string, error) {
	var dirs []string
	pages := s3.NewListObjectsV2Paginator(s.s3, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Delimiter: aws.String("/"),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list root directories: %w", err)
		}
		for _, prefix := range page.CommonPrefixes {
			if name := strings.TrimSuffix(aws.ToString(prefix.Prefix), "/"); name != "" {
				dirs = append(dirs, name)
			}
		}
	}
	return dirs, nil
}

// Index returns every entry under key, recursively and in the order the store
// lists them. key names a directory, so a trailing slash is optional; an
// unknown key yields no entries.
func (s *Storage) Index(ctx context.Context, key string) ([]Entry, error) {
	prefix := strings.TrimSuffix(key, "/") + "/"
	var entries []Entry
	pages := s3.NewListObjectsV2Paginator(s.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("index %s: %w", key, err)
		}
		for _, row := range page.Contents {
			entries = append(entries, Entry{
				Key:  aws.ToString(row.Key),
				ETag: aws.ToString(row.ETag),
			})
		}
	}
	return entries, nil
}

// Get returns the body of key; the caller closes it.
func (s *Storage) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	return resp.Body, nil
}

// Entry is one object: its full key and the ETag exactly as the store reported
// it. The value is opaque - normalizing it would map distinct validators onto
// one value and hide changes.
type Entry struct {
	Key  string
	ETag string
}
