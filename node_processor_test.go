package crdt

import (
	"context"
	"fmt"
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
var nodeProcessorTestCounter int

func createNodeProcessorTestCID(t *testing.T) cid.Cid {
	nodeProcessorTestCounter++
	content := fmt.Sprintf("nodeprocessor-test-%d", nodeProcessorTestCounter)
	node := merkledag.NewRawNode([]byte(content))
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
	require.True(t, result.Ready)
	require.Len(t, result.JobNodes, 1)
	require.Equal(t, rootCid, result.JobNodes[0])
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

func TestNodeProcessor_checkAncestorReadiness_LongChain(t *testing.T) {
	ctx := context.Background()

	// Use existing test infrastructure
	replicas, closeFunc := makeNReplicas(t, 1, nil)
	defer closeFunc()

	store := replicas[0]
	np := store.NewNodeProcessor()

	// Create a long chain: knownBase -> missingA -> missingB -> missingC -> newHead
	// We have knownBase (processed), but missing A, B, C
	knownBaseCid := createNodeProcessorTestCID(t)
	missingCCid := createNodeProcessorTestCID(t)

	// Mark knownBase as processed (we have this in our branch)
	err := store.setBlockState(ctx, knownBaseCid, ProcessedComplete)
	require.NoError(t, err)

	// Create the chain structure: newHead links to missingC, missingC to missingB, etc.
	newHeadNode := newMockTestNode("QmNewHead", missingCCid.String())

	result, err := np.checkAncestorReadiness(ctx, newHeadNode)

	require.NoError(t, err)
	require.False(t, result.AllReady)
	require.Len(t, result.UnprocessedAncestors, 1)
	require.Equal(t, missingCCid, result.UnprocessedAncestors[0])
	require.Empty(t, result.HeadsToReplace) // missingC is not a head
}

func TestNodeProcessor_checkAncestorReadiness_MultipleUnprocessedAncestors(t *testing.T) {
	ctx := context.Background()

	// Use existing test infrastructure
	replicas, closeFunc := makeNReplicas(t, 1, nil)
	defer closeFunc()

	store := replicas[0]
	np := store.NewNodeProcessor()

	// Create a merge node with multiple unprocessed ancestors
	unprocessedA := createNodeProcessorTestCID(t)
	unprocessedB := createNodeProcessorTestCID(t)
	processedC := createNodeProcessorTestCID(t)

	// Mark one ancestor as processed
	err := store.setBlockState(ctx, processedC, ProcessedComplete)
	require.NoError(t, err)

	// Create merge node linking to all three ancestors
	mergeNode := newMockTestNode("QmMerge", unprocessedA.String(), unprocessedB.String(), processedC.String())

	result, err := np.checkAncestorReadiness(ctx, mergeNode)

	require.NoError(t, err)
	require.False(t, result.AllReady)
	require.Len(t, result.UnprocessedAncestors, 2)

	// Check that both unprocessed ancestors are reported
	expectedUnprocessed := map[string]bool{
		unprocessedA.String(): true,
		unprocessedB.String(): true,
	}
	for _, ancestor := range result.UnprocessedAncestors {
		require.True(t, expectedUnprocessed[ancestor.String()], "Unexpected unprocessed ancestor: %s", ancestor)
		delete(expectedUnprocessed, ancestor.String())
	}
	require.Empty(t, expectedUnprocessed, "Missing expected unprocessed ancestors")
}

func TestNodeProcessor_checkAncestorReadiness_CoalescingChains(t *testing.T) {
	ctx := context.Background()

	// Use existing test infrastructure
	replicas, closeFunc := makeNReplicas(t, 1, nil)
	defer closeFunc()

	store := replicas[0]
	np := store.NewNodeProcessor()

	// Create coalescing scenario:
	// HEAD1 -> missingX -> commonBase (we have commonBase)
	// HEAD2 -> missingY -> commonBase (we have commonBase)
	// Test: when we receive HEAD1 and HEAD2, they should both identify missing ancestors

	commonBaseCid := createNodeProcessorTestCID(t)
	missingXCid := createNodeProcessorTestCID(t)
	missingYCid := createNodeProcessorTestCID(t)

	// Mark commonBase as processed (we have this)
	err := store.setBlockState(ctx, commonBaseCid, ProcessedComplete)
	require.NoError(t, err)

	// Also mark it as a head (representing our current state)
	err = store.heads.Add(ctx, commonBaseCid, head{height: 50})
	require.NoError(t, err)

	// Test HEAD1 -> missingX (where missingX -> commonBase, but we don't have missingX)
	head1Node := newMockTestNode("QmHead1", missingXCid.String())

	result1, err := np.checkAncestorReadiness(ctx, head1Node)
	require.NoError(t, err)
	require.False(t, result1.AllReady) // Not ready because missingX is unprocessed
	require.Len(t, result1.UnprocessedAncestors, 1)
	require.Equal(t, missingXCid, result1.UnprocessedAncestors[0])
	require.Len(t, result1.HeadsToReplace, 0) // missingX is not a head

	// Test HEAD2 -> missingY (where missingY -> commonBase, but we don't have missingY)
	head2Node := newMockTestNode("QmHead2", missingYCid.String())

	result2, err := np.checkAncestorReadiness(ctx, head2Node)
	require.NoError(t, err)
	require.False(t, result2.AllReady) // Not ready because missingY is unprocessed
	require.Len(t, result2.UnprocessedAncestors, 1)
	require.Equal(t, missingYCid, result2.UnprocessedAncestors[0])
	require.Len(t, result2.HeadsToReplace, 0) // missingY is not a head

	// Now simulate that we process the missing chain nodes and they link to our common base
	// When missingX is processed and links to commonBase (head), it should be marked for replacement
	err = store.setBlockState(ctx, missingXCid, ProcessedComplete)
	require.NoError(t, err)
	err = store.heads.Add(ctx, missingXCid, head{height: 75}) // Make missingX a head too
	require.NoError(t, err)

	// Now test that the ancestor that became a head is identified for replacement
	result3, err := np.checkAncestorReadiness(ctx, head1Node)
	require.NoError(t, err)
	require.True(t, result3.AllReady) // Now ready because missingX is processed
	require.Empty(t, result3.UnprocessedAncestors)
	require.Len(t, result3.HeadsToReplace, 1) // missingX is now a head and should be replaced
	require.Equal(t, missingXCid, result3.HeadsToReplace[0])
}
