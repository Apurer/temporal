//go:build aws

package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func NewAwsHttpClient(config ESAWSRequestSigningConfig) (*http.Client, error) {
	if !config.Enabled {
		return nil, nil
	}

	if config.Region == "" {
		config.Region = os.Getenv("AWS_REGION")
		if config.Region == "" {
			return nil, fmt.Errorf("unable to resolve AWS region for obtaining AWS Elastic signing credentials")
		}
	}

	credentialsProvider, err := resolveCredentialProvider(config)
	if err != nil {
		return nil, err
	}

	return &http.Client{
		Transport: &awsSigningTransport{
			base:        http.DefaultTransport,
			signer:      v4.NewSigner(),
			credentials: aws.NewCredentialsCache(credentialsProvider),
			region:      config.Region,
			service:     "es",
		},
	}, nil
}

type awsSigningTransport struct {
	base        http.RoundTripper
	signer      *v4.Signer
	credentials *aws.CredentialsCache
	region      string
	service     string
}

func (t *awsSigningTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}

	signedReq, bodyBytes, err := cloneRequest(req)
	if err != nil {
		return nil, err
	}

	awsCreds, err := t.credentials.Retrieve(req.Context())
	if err != nil {
		return nil, err
	}

	err = t.signer.SignHTTP(
		req.Context(),
		awsCreds,
		signedReq,
		payloadHash(bodyBytes),
		t.service,
		t.region,
		time.Now().UTC(),
	)
	if err != nil {
		return nil, err
	}

	return base.RoundTrip(signedReq)
}

func cloneRequest(req *http.Request) (*http.Request, []byte, error) {
	cloned := req.Clone(req.Context())
	if req.Body == nil {
		return cloned, nil, nil
	}

	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, nil, err
	}
	_ = req.Body.Close()

	req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(bodyBytes)), nil
	}

	cloned.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	cloned.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(bodyBytes)), nil
	}

	return cloned, bodyBytes, nil
}

func payloadHash(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}

func resolveCredentialProvider(cfg ESAWSRequestSigningConfig) (aws.CredentialsProvider, error) {
	switch strings.ToLower(cfg.CredentialProvider) {
	case "static":
		return credentials.NewStaticCredentialsProvider(
			cfg.Static.AccessKeyID,
			cfg.Static.SecretAccessKey,
			cfg.Static.Token,
		), nil
	case "environment":
		accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
		if accessKeyID == "" {
			accessKeyID = os.Getenv("AWS_ACCESS_KEY")
		}
		secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		if secretAccessKey == "" {
			secretAccessKey = os.Getenv("AWS_SECRET_KEY")
		}
		if accessKeyID == "" || secretAccessKey == "" {
			return nil, fmt.Errorf("AWS environment credentials are not set")
		}
		return credentials.NewStaticCredentialsProvider(
			accessKeyID,
			secretAccessKey,
			os.Getenv("AWS_SESSION_TOKEN"),
		), nil
	case "", "aws-sdk-default":
		loadedConfig, err := awsconfig.LoadDefaultConfig(
			context.Background(),
			awsconfig.WithRegion(cfg.Region),
		)
		if err != nil {
			return nil, err
		}
		return loadedConfig.Credentials, nil
	default:
		return nil, fmt.Errorf(
			"unknown AWS credential provider specified: %+v. Accepted options are 'static', 'environment' or 'aws-sdk-default'",
			cfg.CredentialProvider,
		)
	}
}
