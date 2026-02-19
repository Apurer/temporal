//go:build aws

package s3store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/searchattribute"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"
)

type S3API interface {
	HeadBucketWithContext(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	HeadObjectWithContext(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	PutObjectWithContext(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObjectWithContext(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2WithContext(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type s3ClientAdapter struct {
	client *s3.Client
}

func (a *s3ClientAdapter) HeadBucketWithContext(ctx context.Context, input *s3.HeadBucketInput, opts ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return a.client.HeadBucket(ctx, input, opts...)
}

func (a *s3ClientAdapter) HeadObjectWithContext(ctx context.Context, input *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return a.client.HeadObject(ctx, input, opts...)
}

func (a *s3ClientAdapter) PutObjectWithContext(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return a.client.PutObject(ctx, input, opts...)
}

func (a *s3ClientAdapter) GetObjectWithContext(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return a.client.GetObject(ctx, input, opts...)
}

func (a *s3ClientAdapter) ListObjectsV2WithContext(ctx context.Context, input *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return a.client.ListObjectsV2(ctx, input, opts...)
}

func newS3Client(cfg *config.S3Archiver) (S3API, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		options.UsePathStyle = cfg.S3ForcePathStyle
		if cfg.Endpoint != nil && *cfg.Endpoint != "" {
			options.BaseEndpoint = aws.String(*cfg.Endpoint)
		}
	})
	return &s3ClientAdapter{client: client}, nil
}

// encoding & decoding util

func Encode(message proto.Message) ([]byte, error) {
	encoder := codec.NewJSONPBEncoder()
	return encoder.Encode(message)
}

func decodeVisibilityRecord(data []byte) (*archiverspb.VisibilityRecord, error) {
	record := &archiverspb.VisibilityRecord{}
	encoder := codec.NewJSONPBEncoder()
	err := encoder.Decode(data, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func SerializeToken(token interface{}) ([]byte, error) {
	if token == nil {
		return nil, nil
	}
	return json.Marshal(token)
}

func deserializeGetHistoryToken(bytes []byte) (*getHistoryToken, error) {
	token := &getHistoryToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func deserializeQueryVisibilityToken(bytes []byte) *string {
	var ret = string(bytes)
	return &ret
}
func serializeQueryVisibilityToken(token string) []byte {
	return []byte(token)
}

// Only validates the scheme and buckets are passed
func SoftValidateURI(URI archiver.URI) error {
	if URI.Scheme() != URIScheme {
		return archiver.ErrURISchemeMismatch
	}
	if len(URI.Hostname()) == 0 {
		return errNoBucketSpecified
	}
	return nil
}

func BucketExists(ctx context.Context, s3cli S3API, URI archiver.URI) error {
	ctx, cancel := ensureContextTimeout(ctx)
	defer cancel()
	_, err := s3cli.HeadBucketWithContext(ctx, &s3.HeadBucketInput{
		Bucket: ptrString(URI.Hostname()),
	})
	if err == nil {
		return nil
	}
	if IsNotFoundError(err) {
		return errBucketNotExists
	}
	return err
}

func KeyExists(ctx context.Context, s3cli S3API, URI archiver.URI, key string) (bool, error) {
	ctx, cancel := ensureContextTimeout(ctx)
	defer cancel()
	_, err := s3cli.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: ptrString(URI.Hostname()),
		Key:    ptrString(key),
	})
	if err != nil {
		if IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func IsNotFoundError(err error) bool {
	code := strings.ToLower(apiErrorCode(err))
	if code == "notfound" || code == "nosuchkey" || code == "nosuchbucket" {
		return true
	}
	status, ok := httpStatusCode(err)
	return ok && status == 404
}

// Key construction
func constructHistoryKey(path, namespaceID, workflowID, runID string, version int64, batchIdx int) string {
	prefix := constructHistoryKeyPrefixWithVersion(path, namespaceID, workflowID, runID, version)
	return fmt.Sprintf("%s%d", prefix, batchIdx)
}

func constructHistoryKeyPrefixWithVersion(path, namespaceID, workflowID, runID string, version int64) string {
	prefix := constructHistoryKeyPrefix(path, namespaceID, workflowID, runID)
	return fmt.Sprintf("%s/%v/", prefix, version)
}

func constructHistoryKeyPrefix(path, namespaceID, workflowID, runID string) string {
	return strings.TrimLeft(strings.Join([]string{path, namespaceID, "history", workflowID, runID}, "/"), "/")
}

func constructTimeBasedSearchKey(path, namespaceID, primaryIndexKey, primaryIndexValue, secondaryIndexKey string, t time.Time, precision string) string {
	var timeFormat = ""
	switch precision {
	case PrecisionSecond:
		timeFormat = ":05"
		fallthrough
	case PrecisionMinute:
		timeFormat = ":04" + timeFormat
		fallthrough
	case PrecisionHour:
		timeFormat = "15" + timeFormat
		fallthrough
	case PrecisionDay:
		timeFormat = "2006-01-02T" + timeFormat
	}

	return fmt.Sprintf(
		"%s/%s",
		constructIndexedVisibilitySearchPrefix(path, namespaceID, primaryIndexKey, primaryIndexValue, secondaryIndexKey),
		t.Format(timeFormat),
	)
}

func constructTimestampIndex(path, namespaceID, primaryIndexKey, primaryIndexValue, secondaryIndexKey string, secondaryIndexValue time.Time, runID string) string {
	return fmt.Sprintf(
		"%s/%s/%s",
		constructIndexedVisibilitySearchPrefix(path, namespaceID, primaryIndexKey, primaryIndexValue, secondaryIndexKey),
		secondaryIndexValue.Format(time.RFC3339),
		runID,
	)
}

func constructIndexedVisibilitySearchPrefix(
	path string,
	namespaceID string,
	primaryIndexKey string,
	primaryIndexValue string,
	secondaryIndexType string,
) string {
	return strings.TrimLeft(
		strings.Join(
			[]string{path, namespaceID, "visibility", primaryIndexKey, primaryIndexValue, secondaryIndexType},
			"/",
		),
		"/",
	)
}

func constructVisibilitySearchPrefix(path, namespaceID string) string {
	return strings.TrimLeft(strings.Join([]string{path, namespaceID, "visibility"}, "/"), "/")
}

func ensureContextTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultBlobstoreTimeout)
}
func Upload(ctx context.Context, s3cli S3API, URI archiver.URI, key string, data []byte) error {
	ctx, cancel := ensureContextTimeout(ctx)
	defer cancel()

	_, err := s3cli.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: ptrString(URI.Hostname()),
		Key:    ptrString(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		if strings.EqualFold(apiErrorCode(err), "NoSuchBucket") {
			return serviceerror.NewInvalidArgument(errBucketNotExists.Error())
		}
		return err
	}
	return nil
}

func Download(ctx context.Context, s3cli S3API, URI archiver.URI, key string) ([]byte, error) {
	ctx, cancel := ensureContextTimeout(ctx)
	defer cancel()
	result, err := s3cli.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: ptrString(URI.Hostname()),
		Key:    ptrString(key),
	})

	if err != nil {
		code := apiErrorCode(err)
		if strings.EqualFold(code, "NoSuchBucket") {
			return nil, serviceerror.NewInvalidArgument(errBucketNotExists.Error())
		}
		if strings.EqualFold(code, "NoSuchKey") || strings.EqualFold(code, "NotFound") {
			return nil, serviceerror.NewNotFound(archiver.ErrHistoryNotExist.Error())
		}
		return nil, err
	}

	defer func() {
		if ierr := result.Body.Close(); ierr != nil {
			err = multierr.Append(err, ierr)
		}
	}()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func ptrString(v string) *string {
	return &v
}

func apiErrorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}

	var noSuchBucket *s3types.NoSuchBucket
	if errors.As(err, &noSuchBucket) {
		return "NoSuchBucket"
	}

	var noSuchKey *s3types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return "NoSuchKey"
	}

	return ""
}

func httpStatusCode(err error) (int, bool) {
	var statusErr interface {
		HTTPStatusCode() int
	}
	if errors.As(err, &statusErr) {
		return statusErr.HTTPStatusCode(), true
	}
	return 0, false
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if isStatusCodeRetryable(err) {
		return true
	}

	code := strings.ToLower(apiErrorCode(err))
	return strings.Contains(code, "throttl") || strings.Contains(code, "timeout") || strings.Contains(code, "temporar")
}

func isStatusCodeRetryable(err error) bool {
	status, ok := httpStatusCode(err)
	if !ok {
		var netErr net.Error
		return errors.As(err, &netErr)
	}
	if status == 429 {
		return true
	}
	return status >= 500 && status != 501
}

func historyMutated(request *archiver.ArchiveHistoryRequest, historyBatches []*historypb.History, isLast bool) bool {
	lastBatch := historyBatches[len(historyBatches)-1].Events
	lastEvent := lastBatch[len(lastBatch)-1]
	lastFailoverVersion := lastEvent.GetVersion()
	if lastFailoverVersion > request.CloseFailoverVersion {
		return true
	}

	if !isLast {
		return false
	}
	lastEventID := lastEvent.GetEventId()
	return lastFailoverVersion != request.CloseFailoverVersion || lastEventID+1 != request.NextEventID
}

func convertToExecutionInfo(record *archiverspb.VisibilityRecord, saTypeMap searchattribute.NameTypeMap) (*workflowpb.WorkflowExecutionInfo, error) {
	searchAttributes, err := searchattribute.Parse(record.SearchAttributes, &saTypeMap)
	if err != nil {
		return nil, err
	}

	return &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: record.GetWorkflowId(),
			RunId:      record.GetRunId(),
		},
		Type: &commonpb.WorkflowType{
			Name: record.WorkflowTypeName,
		},
		StartTime:         record.StartTime,
		ExecutionTime:     record.ExecutionTime,
		CloseTime:         record.CloseTime,
		ExecutionDuration: record.ExecutionDuration,
		Status:            record.Status,
		HistoryLength:     record.HistoryLength,
		Memo:              record.Memo,
		SearchAttributes:  searchAttributes,
	}, nil
}
