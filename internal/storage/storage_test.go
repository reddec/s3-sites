package storage_test

import (
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/reddec/s3-sites/internal/storage"
)

const (
	versityImage = "ghcr.io/versity/versitygw:latest"
	testBucket   = "sites"
	testRegion   = "us-east-1"
	testUser     = "test"
	testSecret   = "testtest"

	// anonymousListPolicy grants everyone the right to list the bucket.
	anonymousListPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::sites"}]}`
)

func TestListReturnsRootSiteDirectories(t *testing.T) {
	env := newEnvironment(t)
	env.put(t, "example.com/index.html", "example")
	env.put(t, "example.com/assets/app.css", "body {}")
	env.put(t, "other.org/index.html", "other")
	env.put(t, "nested-only/deep/file.txt", "no object at its root")
	env.put(t, "stray.txt", "an object, not a directory")

	dirs, err := env.storage.List(t.Context())

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"example.com", "nested-only", "other.org"}, dirs)
}

func TestIndexReturnsEveryEntryUnderKey(t *testing.T) {
	env := newEnvironment(t)
	indexETag := env.put(t, "example.com/index.html", "example")
	cssETag := env.put(t, "example.com/assets/app.css", "body {}")
	svgETag := env.put(t, "example.com/assets/img/logo.svg", "<svg/>")
	env.put(t, "example.com.au/index.html", "a longer sibling name")
	env.put(t, "other.org/index.html", "other")

	entries, err := env.storage.Index(t.Context(), "example.com")

	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"example.com/index.html":          indexETag,
		"example.com/assets/app.css":      cssETag,
		"example.com/assets/img/logo.svg": svgETag,
	}, byKey(entries))

	// key names a directory, so the trailing slash is optional.
	same, err := env.storage.Index(t.Context(), "example.com/")
	require.NoError(t, err)
	assert.Equal(t, entries, same)
}

func TestIndexUnknownKeyReturnsNoEntries(t *testing.T) {
	env := newEnvironment(t)
	env.put(t, "example.com/index.html", "example")

	entries, err := env.storage.Index(t.Context(), "nope.example")

	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestGetStreamsObjectBody(t *testing.T) {
	env := newEnvironment(t)
	env.put(t, "example.com/assets/app.css", "body { color: red }")

	body, err := env.storage.Get(t.Context(), "example.com/assets/app.css")
	require.NoError(t, err)
	defer body.Close()

	content, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Equal(t, "body { color: red }", string(content))
}

func TestGetUnknownKeyFails(t *testing.T) {
	env := newEnvironment(t)

	_, err := env.storage.Get(t.Context(), "example.com/missing.html")

	require.Error(t, err)
	assert.ErrorContains(t, err, "example.com/missing.html")
}

func TestEmptyCredentialsUseUnsignedRequests(t *testing.T) {
	env := newEnvironment(t)
	env.put(t, "example.com/index.html", "example")

	// Granting everyone list access means the unsigned request is served
	// because the store says so, not because this package decided for it.
	_, err := env.seed.PutBucketPolicy(t.Context(), &s3.PutBucketPolicyInput{
		Bucket: aws.String(testBucket),
		Policy: aws.String(anonymousListPolicy),
	})
	require.NoError(t, err)

	store, err := storage.New(t.Context(), storage.Config{
		Endpoint:  env.endpoint,
		Region:    testRegion,
		Bucket:    testBucket,
		PathStyle: true,
	})
	require.NoError(t, err)

	dirs, err := store.List(t.Context())

	require.NoError(t, err)
	assert.Equal(t, []string{"example.com"}, dirs)
}

func TestCredentialsComeFromConfig(t *testing.T) {
	// The environment carries wrong credentials on purpose: a store that
	// ignored its Config would fail to list.
	t.Setenv("AWS_ACCESS_KEY_ID", "wrong")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wrongwrong")
	env := newEnvironment(t)

	dirs, err := env.storage.List(t.Context())

	require.NoError(t, err)
	assert.Empty(t, dirs)
}

func TestEmptyCredentialsIgnoreEnvironment(t *testing.T) {
	// Working credentials in the environment must not leak into a store whose
	// Config carries none: the request goes out as configured and the gateway
	// answers it.
	t.Setenv("AWS_ACCESS_KEY_ID", testUser)
	t.Setenv("AWS_SECRET_ACCESS_KEY", testSecret)
	endpoint := startBucket(t)

	store, err := storage.New(t.Context(), storage.Config{
		Endpoint:  endpoint,
		Region:    testRegion,
		Bucket:    testBucket,
		PathStyle: true,
	})
	require.NoError(t, err)

	_, err = store.List(t.Context())

	require.Error(t, err)
	assert.ErrorContains(t, err, "https response error")
}

func TestCredentialsFromConfigAreUsedAsGiven(t *testing.T) {
	// The environment holds working credentials while Config holds a lone secret:
	// were the secret ignored, the listing would succeed through the chain.
	// Gateways differ in what they accept, so configured values must reach the
	// store as-is instead of being second-guessed here.
	t.Setenv("AWS_ACCESS_KEY_ID", testUser)
	t.Setenv("AWS_SECRET_ACCESS_KEY", testSecret)
	endpoint := startBucket(t)

	store, err := storage.New(t.Context(), storage.Config{
		Endpoint:        endpoint,
		Region:          testRegion,
		Bucket:          testBucket,
		PathStyle:       true,
		SecretAccessKey: testSecret,
	})
	require.NoError(t, err)

	_, err = store.List(t.Context())

	// Only a rejection from the gateway proves the configured secret reached it
	// verbatim rather than being refused locally or replaced by the env pair.
	require.Error(t, err)
	assert.ErrorContains(t, err, "https response error")
}

// environment is a VersityGW bucket plus the store under test. Objects are
// seeded through a second client, so the store stays read-only.
type environment struct {
	storage  *storage.Storage
	seed     *s3.Client
	endpoint string
}

// newEnvironment starts an empty bucket and points a store at it, credentials
// included in the Config.
func newEnvironment(t *testing.T) *environment {
	t.Helper()
	endpoint := startBucket(t)

	store, err := storage.New(t.Context(), storage.Config{
		Endpoint:        endpoint,
		Region:          testRegion,
		Bucket:          testBucket,
		PathStyle:       true,
		AccessKeyID:     testUser,
		SecretAccessKey: testSecret,
	})
	require.NoError(t, err)
	return &environment{storage: store, seed: seedClient(t, endpoint), endpoint: endpoint}
}

// startBucket runs VersityGW holding one empty bucket and returns its endpoint.
func startBucket(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("needs Docker")
	}
	ctx := t.Context()
	ctr, err := testcontainers.Run(ctx, versityImage,
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithCmd("-p", ":9000", "-a", testUser, "-s", testSecret, "posix", "/data"),
		testcontainers.WithTmpfs(map[string]string{"/data": ""}),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("9000/tcp")),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	endpoint, err := ctr.PortEndpoint(ctx, "9000/tcp", "http")
	require.NoError(t, err)
	_, err = seedClient(t, endpoint).CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(testBucket)})
	require.NoError(t, err)
	return endpoint
}

// seedClient writes to the bucket with static test credentials, independent of
// how the store under test resolves its own.
func seedClient(t *testing.T, endpoint string) *s3.Client {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(t.Context(),
		awsconfig.WithRegion(testRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(testUser, testSecret, "")),
	)
	require.NoError(t, err)
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = new(endpoint)
		o.UsePathStyle = true
	})
}

// put seeds an object and returns the raw ETag the store reported for it.
func (e *environment) put(t *testing.T, key, content string) string {
	t.Helper()
	out, err := e.seed.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(content),
	})
	require.NoError(t, err)
	return aws.ToString(out.ETag)
}

// byKey folds entries into a key -> ETag map so a whole Index result compares
// as one value, independent of listing order.
func byKey(entries []storage.Entry) map[string]string {
	indexed := make(map[string]string, len(entries))
	for _, entry := range entries {
		indexed[entry.Key] = entry.ETag
	}
	return indexed
}
