//go:build aws

package s3store

import "github.com/aws/smithy-go"

type apiTestError struct {
	code       string
	statusCode int
}

func (e *apiTestError) Error() string {
	return e.code
}

func (e *apiTestError) ErrorCode() string {
	return e.code
}

func (e *apiTestError) ErrorMessage() string {
	return e.code
}

func (e *apiTestError) ErrorFault() smithy.ErrorFault {
	return smithy.FaultClient
}

func (e *apiTestError) HTTPStatusCode() int {
	return e.statusCode
}

func newAPIError(code string, statusCode int) error {
	return &apiTestError{
		code:       code,
		statusCode: statusCode,
	}
}
