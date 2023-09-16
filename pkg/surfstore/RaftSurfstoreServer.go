package surfstore

import (
	context "context"
	"fmt"
	"math"
	"sync"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	ip             string
	ipList         []string
	serverId       int64
	pendingCommits []chan bool
	commitIndex    int64
	lastApplied    int64
	nextIndices    []int64
	matchIndices   []int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	// block until majority of servers are working
	// for {
	// 	majority, _ := s.SendHeartbeat(ctx, empty)
	// 	if majority.Flag {
	// 		break
	// 	}
	// }
	majority, _ := s.SendHeartbeat(ctx, empty)
	if !majority.Flag {
		return nil, fmt.Errorf("majority servers crashed")
	}

	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	// block until majority of servers are working
	empty := &emptypb.Empty{}
	// for {
	// 	majority, _ := s.SendHeartbeat(ctx, empty)
	// 	if majority.Flag {
	// 		break
	// 	}
	// }
	majority, _ := s.SendHeartbeat(ctx, empty)
	if !majority.Flag {
		return nil, fmt.Errorf("majority servers crashed")
	}

	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	// block until majority of servers are working
	// for {
	// 	majority, _ := s.SendHeartbeat(ctx, empty)
	// 	if majority.Flag {
	// 		break
	// 	}
	// }
	majority, _ := s.SendHeartbeat(ctx, empty)
	if !majority.Flag {
		return nil, fmt.Errorf("majority servers crashed")
	}

	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
}

func (s *RaftSurfstore) sendAppendEntry(ctx context.Context, address string, responses chan bool) {
	var (
		PrevLogIndex int64
		PrevLogTerm  int64
	)

	var serverIndex int64
	for i, addr := range s.ipList {
		if addr == address {
			serverIndex = int64(i)
			break
		}
	}

	if s.nextIndices[serverIndex] >= 1 {
		PrevLogIndex = s.nextIndices[serverIndex] - 1
		PrevLogTerm = s.log[PrevLogIndex].Term
	} else {
		PrevLogIndex = -1
		PrevLogTerm = 0
	}

	input := &AppendEntryInput{
		Term:         s.term,
		LeaderCommit: s.lastApplied,
		PrevLogIndex: PrevLogIndex,
		PrevLogTerm:  PrevLogTerm,
		Entries:      s.log[s.nextIndices[serverIndex]:],
	}

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		responses <- false
		return
	}
	defer conn.Close()

	client := NewRaftSurfstoreClient(conn)
	resp, err := client.AppendEntries(ctx, input)

	if err != nil {
		responses <- false
		return
	} else if resp.Term > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
		responses <- false
		return
	} else if resp.Success {
		s.nextIndices[resp.ServerId] = resp.MatchedIndex + 1
		s.matchIndices[resp.ServerId] = resp.MatchedIndex
		responses <- true
		return
	} else {
		s.nextIndices[resp.ServerId] -= 1
		s.matchIndices[resp.ServerId] -= 1
		responses <- false
		return
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	operation := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	s.log = append(s.log, &operation)
	// committed := make(chan bool)
	// s.pendingCommits = append(s.pendingCommits, committed)

	for {
		s.isCrashedMutex.RLock()
		isCrashed := s.isCrashed
		s.isCrashedMutex.RUnlock()
		if isCrashed {
			return nil, ERR_SERVER_CRASHED
		}
		s.isLeaderMutex.RLock()
		isLeader := s.isLeader
		s.isLeaderMutex.RUnlock()
		if !isLeader {
			return nil, ERR_NOT_LEADER
		}

		responses := make(chan bool, len(s.ipList)-1)

		for index, address := range s.ipList {
			if int64(index) == s.serverId {
				continue
			}
			go s.sendAppendEntry(ctx, address, responses)
		}

		appendCount := 1
		total := 0

		for {
			value := <-responses
			if value {
				appendCount++
			}
			total++
			if total == len(s.ipList)-1 {
				break
			}
		}

		commit := false

		for n := int64(len(s.log) - 1); n > s.commitIndex; n-- {
			if s.log[n].Term == s.term {
				count := 1
				for _, matchIdx := range s.matchIndices {
					if matchIdx >= n {
						count++
					}
				}
				if count > len(s.ipList)/2 {
					s.commitIndex = n
					commit = true
					break
				}
			}
		}

		if commit {
			if s.commitIndex == int64(len(s.log)-1) {
				for i := s.lastApplied + 1; i < s.commitIndex; i++ {
					s.metaStore.UpdateFile(ctx, s.log[i].FileMetaData)
				}
				s.lastApplied = s.commitIndex
				return s.metaStore.UpdateFile(ctx, filemeta)
			}
		}
		// sleep?
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	output := &AppendEntryOutput{
		Term:     s.term,
		ServerId: s.serverId,
		Success:  false,
	}

	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return output, ERR_SERVER_CRASHED
	}

	if input.Term > s.term {
		s.term = input.Term
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
	}

	// 1??
	if input.Term < s.term {
		return output, nil
	}

	// 2
	if input.PrevLogIndex != -1 && len(s.log) > int(input.PrevLogIndex) && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
		return output, nil
	}

	// 3 delete entries that do not have same term after
	if input.PrevLogIndex+1 < int64(len(s.log)) {
		s.log = s.log[:input.PrevLogIndex+1]
	}

	// 4
	s.log = append(s.log, input.Entries...)

	// 5
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))

		for s.lastApplied < s.commitIndex {
			s.lastApplied++
			s.metaStore.UpdateFile(ctx, s.log[s.lastApplied].FileMetaData)
		}
	}
	output.Success = true
	output.MatchedIndex = int64(len(s.log) - 1)
	// output.MatchedIndex = s.commitIndex
	return output, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	s.term = s.term + 1
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()
	for i := range s.ipList {
		s.nextIndices[int64(i)] = int64(len(s.log))
		s.matchIndices[int64(i)] = -1
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) sendBeatToOthers(ctx context.Context, address string, responses chan bool) {
	var (
		PrevLogIndex int64
		PrevLogTerm  int64
	)

	var serverIndex int64
	for index, ip := range s.ipList {
		if ip == address {
			serverIndex = int64(index)
			break
		}
	}

	if s.nextIndices[serverIndex] >= 1 {
		PrevLogIndex = s.nextIndices[serverIndex] - 1
		PrevLogTerm = s.log[PrevLogIndex].Term
	} else {
		PrevLogIndex = -1
		PrevLogTerm = 0
	}

	input := &AppendEntryInput{
		Term:         s.term,
		LeaderCommit: s.lastApplied,
		PrevLogIndex: PrevLogIndex,
		PrevLogTerm:  PrevLogTerm,
		Entries:      s.log[s.nextIndices[serverIndex]:],
	}

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		responses <- false
		return
	}
	defer conn.Close()
	client := NewRaftSurfstoreClient(conn)
	resp, err := client.AppendEntries(ctx, input)

	if err != nil {
		responses <- false
		return
	} else if resp.Term > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
		responses <- false
		return
	} else if resp.Success {
		s.nextIndices[resp.ServerId] = resp.MatchedIndex + 1
		s.matchIndices[resp.ServerId] = resp.MatchedIndex
		responses <- true
		return
	} else {
		s.nextIndices[resp.ServerId] -= 1
		s.matchIndices[resp.ServerId] -= 1
		responses <- true
		return
	}
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	responses := make(chan bool, len(s.ipList)-1)

	for index, address := range s.ipList {
		if int64(index) == s.serverId {
			continue
		}
		go s.sendBeatToOthers(ctx, address, responses)
	}

	appendCount := 1
	total := 0

	for {
		value := <-responses
		if value {
			appendCount++
		}
		total++
		if total == len(s.ipList)-1 {
			break
		}
	}

	for n := int64(len(s.log) - 1); n > s.commitIndex; n-- {
		if s.log[n].Term == s.term {
			count := 1
			for _, index := range s.matchIndices {
				if index >= n {
					count++
				}
			}
			if count > len(s.ipList)/2 {
				s.commitIndex = n
				break
			}
		}
	}

	s.isLeaderMutex.RLock()
	isLeader = s.isLeader
	s.isLeaderMutex.RUnlock()

	if isLeader && appendCount > len(s.ipList)/2 {
		return &Success{Flag: true}, nil
	}
	return &Success{Flag: false}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
