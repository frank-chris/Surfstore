package SurfTest

import (
	context "context"
	"cse224/proj5/pkg/surfstore"
	"fmt"
	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type TestInfo struct {
	CfgPath    string
	Ips        []string
	Context    context.Context
	CancelFunc context.CancelFunc
	Procs      []*exec.Cmd
	Conns      []*grpc.ClientConn
	Clients    []surfstore.RaftSurfstoreClient
}

func InitTest(cfgPath string) TestInfo {
	cfg := surfstore.LoadRaftConfigFile(cfgPath)

	procs := make([]*exec.Cmd, 0)
	procs = append(procs, InitBlockStores(cfg.BlockAddrs)...)
	procs = append(procs, InitRaftServers(cfgPath, cfg)...)

	conns := make([]*grpc.ClientConn, 0)
	clients := make([]surfstore.RaftSurfstoreClient, 0)
	for _, addr := range cfg.RaftAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Fatal("Error connecting to clients ", err)
		}
		client := surfstore.NewRaftSurfstoreClient(conn)

		conns = append(conns, conn)
		clients = append(clients, client)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)

	return TestInfo{
		CfgPath:    cfgPath,
		Ips:        cfg.RaftAddrs,
		Context:    ctx,
		CancelFunc: cancel,
		Procs:      procs,
		Conns:      conns,
		Clients:    clients,
	}
}

func EndTest(test TestInfo) {
	test.CancelFunc()

	for _, server := range test.Procs {
		_ = server.Process.Kill()
	}

	exec.Command("pkill SurfstoreRaftServerExec*")

	for _, conn := range test.Conns {
		conn.Close()
	}

	// saw that sometime a test would fail right away saying "connection already in use"
	time.Sleep(100 * time.Millisecond)
}

func InitBlockStores(blockStoreAddrs []string) []*exec.Cmd {
	blockCmdList := make([]*exec.Cmd, 0)
	for _, addr := range blockStoreAddrs {
		port := strings.Split(addr, ":")[1]
		blockCmd := exec.Command("_bin/SurfstoreServerExec", "-d", "-s", "block", "-p", port, "-l")
		blockCmd.Stderr = os.Stderr
		blockCmd.Stdout = os.Stdout
		err := blockCmd.Start()
		if err != nil {
			log.Fatal("Error starting BlockStore ", err)
		}
		blockCmdList = append(blockCmdList, blockCmd)
	}

	return blockCmdList
}

func InitRaftServers(cfgPath string, cfg surfstore.RaftConfig) []*exec.Cmd {
	cmdList := make([]*exec.Cmd, 0)
	for idx, _ := range cfg.RaftAddrs {

		cmd := exec.Command("_bin/SurfstoreRaftServerExec", "-f", cfgPath, "-i", strconv.Itoa(idx))
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		cmdList = append(cmdList, cmd)
	}

	for _, cmd := range cmdList {
		err := cmd.Start()
		if err != nil {
			log.Fatal("Error starting servers", err)
		}
	}

	time.Sleep(2 * time.Second)

	return cmdList
}

func CheckInternalState(isLeader *bool, term *int64, log []*surfstore.UpdateOperation, fileMetaMap map[string]*surfstore.FileMetaData, server surfstore.RaftSurfstoreClient, ctx context.Context) (bool, error) {
	state, err := server.GetInternalState(ctx, &emptypb.Empty{})
	if err != nil {
		return false, fmt.Errorf("could not get internal state: %w", err)
	}
	if state == nil {
		return false, fmt.Errorf("state is nil")
	}
	if isLeader != nil && *isLeader != state.IsLeader {
		return false, fmt.Errorf("expected leader state %t, got %t", *isLeader, state.IsLeader)
	}
	if term != nil && *term != state.Term {
		return false, fmt.Errorf("expected term %d, got %d", *term, state.Term)
	}
	if log != nil && !SameLog(log, state.Log) {
		return false, fmt.Errorf("incorrect log")
	}
	if fileMetaMap != nil && !SameMeta(fileMetaMap, state.MetaMap.FileInfoMap) {
		return false, fmt.Errorf("incorrect MetaStore state")
	}

	return true, nil
}

func SameOperation(op1, op2 *surfstore.UpdateOperation) bool {
	if op1 == nil && op2 == nil {
		return true
	}
	if op1 == nil || op2 == nil {
		return false
	}
	if op1.Term != op2.Term {
		return false
	}
	if op1.FileMetaData == nil && op2.FileMetaData != nil ||
		op1.FileMetaData != nil && op2.FileMetaData == nil {
		return false
	}
	if op1.FileMetaData.Version != op2.FileMetaData.Version {
		return false
	}
	if !SameHashList(op1.FileMetaData.BlockHashList, op2.FileMetaData.BlockHashList) {
		return false
	}
	return true
}

func SameLog(log1, log2 []*surfstore.UpdateOperation) bool {
	if len(log1) != len(log2) {
		return false
	}
	for idx, entry1 := range log1 {
		entry2 := log2[idx]
		if !SameOperation(entry1, entry2) {
			return false
		}
	}
	return true
}
