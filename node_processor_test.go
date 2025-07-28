package crdt

import (
	"context"
	"testing"

	"github.com/ipfs/boxo/ipld/merkledag"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/require"
)

// Mock IPLD Node for testing
type mockTestNode struct {
	cid   cid.Cid
	links []*ipld.Link
}

func (mn *mockTestNode) Cid() cid.Cid                                         { return mn.cid }
func (mn *mockTestNode) Links() []*ipld.Link                                  { return mn.links }
func (mn *mockTestNode) Resolve(path []string) (interface{}, []string, error) { return nil, nil, nil }
func (mn *mockTestNode) Tree(path string, depth int) []string                 { return nil }
func (mn *mockTestNode) ResolveLink(path []string) (*ipld.Link, []string, error) {
	return nil, nil, nil
}
func (mn *mockTestNode) Copy() ipld.Node { return mn }
func (mn *mockTestNode) String() string  { return mn.cid.String() }
func (mn *mockTestNode) Loggable() map[string]interface{} {
	return map[string]interface{}{"cid": mn.cid}
}
func (mn *mockTestNode) RawData() []byte               { return nil }
func (mn *mockTestNode) Size() (uint64, error)         { return 0, nil }
func (mn *mockTestNode) Stat() (*ipld.NodeStat, error) { return nil, nil }

func newMockTestNode(cidStr string, linkCids ...string) *mockTestNode {
	c, _ := cid.Parse(cidStr)
	var links []*ipld.Link
	for _, linkCid := range linkCids {
		lc, _ := cid.Parse(linkCid)
		links = append(links, &ipld.Link{Cid: lc})
	}
	return &mockTestNode{cid: c, links: links}
}

// Helper to create a test CID for NodeProcessor tests
func createNodeProcessorTestCID(t *testing.T) cid.Cid {
	node := merkledag.NewRawNode([]byte("nodeprocessor-test"))
	return node.Cid()
}

func TestNodeProcessor_Creation(t *testing.T) {
	// Use existing test infrastructure from the codebase
	replicas, closeFunc := makeNReplicas(t, 1, nil)
	defer closeFunc()

	store := replicas[0]

	// Test NodeProcessor creation
	np := store.NewNodeProcessor()
	require.NotNil(t, np)
	require.Equal(t, store, np.store)
}

func TestNodeProcessor_checkAncestorReadiness_NoLinks(t *testing.T) {
	ctx := context.Background()

	// Use existing test infrastructure
	replicas, closeFunc := makeNReplicas(t, 1, nil)
	defer closeFunc()

	store := replicas[0]
	np := store.NewNodeProcessor()

	// Create a node with no links
	node := newMockTestNode("QmRootCid")

	result, err := np.checkAncestorReadiness(ctx, node)

	require.NoError(t, err)
	require.True(t, result.AllReady)
	require.Empty(t, result.UnprocessedAncestors)
	require.Empty(t, result.HeadsToReplace)
}

func TestNodeProcessor_checkAncestorReadiness_WithProcessedAncestor(t *testing.T) {
	ctx := context.Background()

	// Use existing test infrastructure
	replicas, closeFunc := makeNReplicas(t, 1, nil)
	defer closeFunc()

	store := replicas[0]
	np := store.NewNodeProcessor()

	// Create ancestor CID and mark it as processed
	ancestorCid := createNodeProcessorTestCID(t)
	err := store.setBlockState(ctx, ancestorCid, ProcessedComplete)
	require.NoError(t, err)

	// Create a node with the processed ancestor
	node := newMockTestNode("QmRootCid", ancestorCid.String())

	result, err := np.checkAncestorReadiness(ctx, node)

	require.NoError(t, err)
	require.True(t, result.AllReady)
	require.Empty(t, result.UnprocessedAncestors)
	require.Empty(t, result.HeadsToReplace) // Not a head, so won't be replaced
}

func TestNodeProcessor_checkAncestorReadiness_WithUnprocessedAncestor(t *testing.T) {
	ctx := context.Background()

	// Use existing test infrastructure
	replicas, closeFunc := makeNReplicas(t, 1, nil)
	defer closeFunc()

	store := replicas[0]
	np := store.NewNodeProcessor()

	// Create ancestor CID but don't process it (default state is unprocessed)
	ancestorCid := createNodeProcessorTestCID(t)

	// Create a node with the unprocessed ancestor
	node := newMockTestNode("QmRootCid", ancestorCid.String())

	result, err := np.checkAncestorReadiness(ctx, node)

	require.NoError(t, err)
	require.False(t, result.AllReady)
	require.Len(t, result.UnprocessedAncestors, 1)
	require.Equal(t, ancestorCid, result.UnprocessedAncestors[0])
	require.Empty(t, result.HeadsToReplace)
}

func TestNodeProcessor_checkJobReadiness_NoJob(t *testing.T) {
	ctx := context.Background()

	// Use existing test infrastructure
	replicas, closeFunc := makeNReplicas(t, 1, nil)
	defer closeFunc()

	store := replicas[0]
	np := store.NewNodeProcessor()

	// Test with a CID that has no job
	rootCid := createNodeProcessorTestCID(t)

	result, err := np.checkJobReadiness(ctx, rootCid)

	require.NoError(t, err)
	require.False(t, result.Ready)
	require.Empty(t, result.JobNodes)
}

func TestNodeProcessor_updateHeads_AddNew(t *testing.T) {
	ctx := context.Background()

	// Use existing test infrastructure
	replicas, closeFunc := makeNReplicas(t, 1, nil)
	defer closeFunc()

	store := replicas[0]
	np := store.NewNodeProcessor()

	// Test adding a new head (no heads to replace)
	rootCid := createNodeProcessorTestCID(t)
	rootPrio := uint64(100)

	err := np.updateHeads(ctx, rootCid, rootPrio, []cid.Cid{})

	require.NoError(t, err)

	// Verify head was added
	isHead, height, err := store.heads.IsHead(ctx, rootCid)
	require.NoError(t, err)
	require.True(t, isHead)
	require.Equal(t, rootPrio, height)
}

func TestNodeProcessor_updateHeads_ReplaceExisting(t *testing.T) {
	ctx := context.Background()

	// Use existing test infrastructure
	replicas, closeFunc := makeNReplicas(t, 1, nil)
	defer closeFunc()

	store := replicas[0]
	np := store.NewNodeProcessor()

	// Add an existing head first
	oldHeadCid := createNodeProcessorTestCID(t)
	err := store.heads.Add(ctx, oldHeadCid, head{height: 50})
	require.NoError(t, err)

	// Test replacing the existing head
	rootCid := createNodeProcessorTestCID(t)
	rootPrio := uint64(100)
	headsToReplace := []cid.Cid{oldHeadCid}

	err = np.updateHeads(ctx, rootCid, rootPrio, headsToReplace)

	require.NoError(t, err)

	// Verify old head was removed and new head was added
	isOldHead, _, err := store.heads.IsHead(ctx, oldHeadCid)
	require.NoError(t, err)
	require.False(t, isOldHead)

	isNewHead, height, err := store.heads.IsHead(ctx, rootCid)
	require.NoError(t, err)
	require.True(t, isNewHead)
	require.Equal(t, rootPrio, height)
}
