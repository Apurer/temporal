//go:generate mockgen -build_flags=--tags=aws -package "$GOPACKAGE" -destination s3api.go go.temporal.io/server/common/archiver/s3store S3API

package mocks
