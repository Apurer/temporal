//go:build !aws

package client

import (
	"errors"
	"net/http"
)

var errAWSRequestSigningDisabled = errors.New("elasticsearch AWS request signing is disabled in hardened build; rebuild with -tags aws to enable")

func NewAwsHttpClient(config ESAWSRequestSigningConfig) (*http.Client, error) {
	if !config.Enabled {
		return nil, nil
	}
	return nil, errAWSRequestSigningDisabled
}
