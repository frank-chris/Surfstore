package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftInterface interface {
	AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error)
	SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error)
	SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error)
}

type RaftTestingInterface interface {
	GetInternalState(ctx context.Context, _ *emptypb.Empty) (*RaftInternalState, error)
	Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error)
	Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error)
}

type RaftSurfstoreInterface interface {
	MetaStoreInterface
	RaftInterface
	RaftTestingInterface
}
