package persistence

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
)

var (
	// ErrInvalidMembershipExpiry is used when upserting new cluster membership with an invalid duration
	ErrInvalidMembershipExpiry = errors.New("membershipExpiry duration should be atleast 1 second")

	// ErrIncompleteMembershipUpsert is used when upserting new cluster membership with missing fields
	ErrIncompleteMembershipUpsert = errors.New("membership upserts require all fields")
)

type (
	// clusterMetadataManagerImpl implements MetadataManager based on MetadataStore and Serializer
	clusterMetadataManagerImpl struct {
		serializer         serialization.Serializer
		persistence        ClusterMetadataStore
		currentClusterName string
		logger             log.Logger
	}
)

var _ ClusterMetadataManager = (*clusterMetadataManagerImpl)(nil)

// NewClusterMetadataManagerImpl returns new ClusterMetadataManager
func NewClusterMetadataManagerImpl(
	persistence ClusterMetadataStore,
	serializer serialization.Serializer,
	currentClusterName string,
	logger log.Logger,
) ClusterMetadataManager {
	return &clusterMetadataManagerImpl{
		serializer:         serializer,
		persistence:        persistence,
		currentClusterName: currentClusterName,
		logger:             logger,
	}
}

func (m *clusterMetadataManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *clusterMetadataManagerImpl) Close() {
	m.persistence.Close()
}

func (m *clusterMetadataManagerImpl) GetClusterMembers(
	ctx context.Context,
	request *GetClusterMembersRequest,
) (*GetClusterMembersResponse, error) {
	return m.persistence.GetClusterMembers(ctx, request)
}

func (m *clusterMetadataManagerImpl) UpsertClusterMembership(
	ctx context.Context,
	request *UpsertClusterMembershipRequest,
) error {
	if request.RecordExpiry.Seconds() < 1 {
		return ErrInvalidMembershipExpiry
	}
	if request.Role == All {
		return ErrIncompleteMembershipUpsert
	}
	if request.RPCAddress == nil {
		return ErrIncompleteMembershipUpsert
	}
	if request.RPCPort == 0 {
		return ErrIncompleteMembershipUpsert
	}
	if request.SessionStart.IsZero() {
		return ErrIncompleteMembershipUpsert
	}

	return m.persistence.UpsertClusterMembership(ctx, request)
}

func (m *clusterMetadataManagerImpl) PruneClusterMembership(
	ctx context.Context,
	request *PruneClusterMembershipRequest,
) error {
	return m.persistence.PruneClusterMembership(ctx, request)
}

func (m *clusterMetadataManagerImpl) ListClusterMetadata(
	ctx context.Context,
	request *ListClusterMetadataRequest,
) (*ListClusterMetadataResponse, error) {
	resp, err := m.persistence.ListClusterMetadata(ctx, &InternalListClusterMetadataRequest{
		PageSize:      request.PageSize,
		NextPageToken: request.NextPageToken,
	})
	if err != nil {
		return nil, err
	}

	clusterMetadata := make([]*GetClusterMetadataResponse, 0, len(resp.ClusterMetadata))
	for _, cm := range resp.ClusterMetadata {
		res, err := m.convertInternalGetClusterMetadataResponse(cm)
		if err != nil {
			return nil, err
		}
		clusterMetadata = append(clusterMetadata, res)
	}
	return &ListClusterMetadataResponse{ClusterMetadata: clusterMetadata, NextPageToken: resp.NextPageToken}, nil
}

func (m *clusterMetadataManagerImpl) GetCurrentClusterMetadata(
	ctx context.Context,
) (*GetClusterMetadataResponse, error) {
	resp, err := m.persistence.GetClusterMetadata(ctx, &InternalGetClusterMetadataRequest{ClusterName: m.currentClusterName})
	if err != nil {
		return nil, err
	}

	mcm, err := m.serializer.DeserializeClusterMetadata(resp.ClusterMetadata)
	if err != nil {
		return nil, err
	}
	return &GetClusterMetadataResponse{ClusterMetadata: mcm, Version: resp.Version}, nil
}

func (m *clusterMetadataManagerImpl) GetClusterMetadata(
	ctx context.Context,
	request *GetClusterMetadataRequest,
) (*GetClusterMetadataResponse, error) {
	resp, err := m.persistence.GetClusterMetadata(ctx, &InternalGetClusterMetadataRequest{ClusterName: request.ClusterName})
	if err != nil {
		return nil, err
	}

	mcm, err := m.serializer.DeserializeClusterMetadata(resp.ClusterMetadata)
	if err != nil {
		return nil, err
	}
	return &GetClusterMetadataResponse{ClusterMetadata: mcm, Version: resp.Version}, nil
}

func (m *clusterMetadataManagerImpl) SaveClusterMetadata(
	ctx context.Context,
	request *SaveClusterMetadataRequest,
) (bool, error) {
	mcm, err := m.serializer.SerializeClusterMetadata(request.ClusterMetadata)
	if err != nil {
		return false, err
	}

	oldClusterMetadata, err := m.GetClusterMetadata(ctx, &GetClusterMetadataRequest{ClusterName: request.GetClusterName()})
	if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
		return m.persistence.SaveClusterMetadata(ctx, &InternalSaveClusterMetadataRequest{
			ClusterName:     request.ClusterName,
			ClusterMetadata: mcm,
			Version:         request.Version,
		})
	}
	if err != nil {
		return false, err
	}
	if immutableFieldsChanged(oldClusterMetadata.ClusterMetadata, request.ClusterMetadata) {
		return false, nil
	}

	return m.persistence.SaveClusterMetadata(ctx, &InternalSaveClusterMetadataRequest{
		ClusterName:     request.ClusterName,
		ClusterMetadata: mcm,
		Version:         request.Version,
	})
}

func (m *clusterMetadataManagerImpl) DeleteClusterMetadata(
	ctx context.Context,
	request *DeleteClusterMetadataRequest,
) error {
	if request.ClusterName == m.currentClusterName {
		return serviceerror.NewInvalidArgument("Cannot delete current cluster metadata")
	}

	return m.persistence.DeleteClusterMetadata(ctx, &InternalDeleteClusterMetadataRequest{ClusterName: request.ClusterName})
}

func (m *clusterMetadataManagerImpl) convertInternalGetClusterMetadataResponse(
	resp *InternalGetClusterMetadataResponse,
) (*GetClusterMetadataResponse, error) {
	mcm, err := m.serializer.DeserializeClusterMetadata(resp.ClusterMetadata)
	if err != nil {
		return nil, err
	}

	return &GetClusterMetadataResponse{
		ClusterMetadata: mcm,
		Version:         resp.Version,
	}, nil
}

// immutableFieldsChanged returns true if any of immutable fields changed.
func immutableFieldsChanged(old *persistencespb.ClusterMetadata, cur *persistencespb.ClusterMetadata) bool {
	if (old.ClusterName != "" && old.ClusterName != cur.ClusterName) ||
		(old.ClusterId != "" && old.ClusterId != cur.ClusterId) ||
		(old.HistoryShardCount != 0 && old.HistoryShardCount != cur.HistoryShardCount) ||
		(old.IsGlobalNamespaceEnabled && !cur.IsGlobalNamespaceEnabled) {
		return true
	}
	if old.IsGlobalNamespaceEnabled {
		if (old.FailoverVersionIncrement != 0 && old.FailoverVersionIncrement != cur.FailoverVersionIncrement) ||
			(old.InitialFailoverVersion != 0 && old.InitialFailoverVersion != cur.InitialFailoverVersion) {
			return true
		}
	}
	return false
}
