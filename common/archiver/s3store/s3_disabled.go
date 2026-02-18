//go:build !aws

package s3store

import (
	"errors"

	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

const (
	// URIScheme is the scheme for the s3 implementation.
	URIScheme = "s3"
)

var errS3ArchiverDisabled = errors.New("s3 archiver support is disabled in hardened build; rebuild with -tags aws to enable")

func NewHistoryArchiver(
	_ persistence.ExecutionManager,
	_ log.Logger,
	_ metrics.Handler,
	_ *config.S3Archiver,
) (archiver.HistoryArchiver, error) {
	return nil, errS3ArchiverDisabled
}

func NewVisibilityArchiver(
	_ log.Logger,
	_ metrics.Handler,
	_ *config.S3Archiver,
) (archiver.VisibilityArchiver, error) {
	return nil, errS3ArchiverDisabled
}
