package crdt

import (
	"context"
	"fmt"

	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// NodeProcessor handles the processing of CRDT nodes with separated concerns
type NodeProcessor struct {
	store *Datastore
}

// NewNodeProcessor creates a new NodeProcessor
func (store *Datastore) NewNodeProcessor() *NodeProcessor {
	return &NodeProcessor{store: store}
}

// AncestorCheckResult contains the result of checking ancestor readiness
type AncestorCheckResult struct {
	AllReady             bool
	UnprocessedAncestors []cid.Cid
	HeadsToReplace       []cid.Cid
}

// checkAncestorReadiness validates that all ancestor nodes are ready for processing
func (np *NodeProcessor) checkAncestorReadiness(ctx context.Context, node ipld.Node) (*AncestorCheckResult, error) {
	result := &AncestorCheckResult{
		AllReady:             true,
		UnprocessedAncestors: []cid.Cid{},
		HeadsToReplace:       []cid.Cid{},
	}

	for _, link := range node.Links() {
		ancestorCID := link.Cid

		bs, err := np.store.getBlockState(ctx, ancestorCID)
		if err != nil {
			return nil, fmt.Errorf("error checking ancestor state %s: %w", ancestorCID, err)
		}

		isFetching := bs == FetchRequested
		isProcessed := bs == ProcessedComplete
		if !isFetching && !isProcessed {
			result.AllReady = false
			result.UnprocessedAncestors = append(result.UnprocessedAncestors, ancestorCID)
		}

		if ok, _, err := np.store.heads.IsHead(ctx, ancestorCID); err == nil && ok {
			result.HeadsToReplace = append(result.HeadsToReplace, ancestorCID)
		} else if err != nil {
			return nil, fmt.Errorf("error checking ancestor is head %s: %w", ancestorCID, err)
		}
	}

	return result, nil
}

// JobReadinessResult contains the result of checking job readiness
type JobReadinessResult struct {
	Ready    bool
	JobNodes []cid.Cid
}

// checkJobReadiness determines if a job is ready for processing
func (np *NodeProcessor) checkJobReadiness(ctx context.Context, root cid.Cid) (*JobReadinessResult, error) {
	jobNodes, ready, err := np.store.isJobReady(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("error checking if job is ready: %w", err)
	}

	return &JobReadinessResult{
		Ready:    ready,
		JobNodes: jobNodes,
	}, nil
}

// mergeJobDeltas processes and merges all deltas in a job
func (np *NodeProcessor) mergeJobDeltas(ctx context.Context, root cid.Cid, jobNodes []cid.Cid, getter *crdtNodeGetter) error {
	np.store.logger.Debugf("Job '%s' is ready, processing %d nodes", root, len(jobNodes))

	for _, child := range jobNodes {
		if err := np.mergeSingleDelta(ctx, root, child, getter); err != nil {
			return err
		}
	}

	if err := np.store.completeJob(ctx, root); err != nil {
		np.store.logger.Errorf("error updating job progress: %s", err)
	}

	return nil
}

// mergeSingleDelta merges a single child delta and updates its status
func (np *NodeProcessor) mergeSingleDelta(ctx context.Context, root, child cid.Cid, getter *crdtNodeGetter) error {
	blockKey := dshelp.MultihashToDsKey(child.Hash()).String()

	_, childDelta, err := getter.GetDelta(ctx, child)
	if err != nil {
		return fmt.Errorf("error getting delta for %s: %w", child, err)
	}

	if err := np.store.set.Merge(ctx, childDelta, blockKey); err != nil {
		return fmt.Errorf("error merging delta from %s: %w", child, err)
	}

	if err := np.store.markProcessed(ctx, child); err != nil {
		return fmt.Errorf("error recording %s as processed: %w", child, err)
	}

	np.store.queuedChildren.Remove(child)

	err = np.store.setChildStatus(ctx, root, child, ChildProcessed)
	if err != nil {
		return fmt.Errorf("error recording %s as processed: %w", child, err)
	}

	if prio := childDelta.GetPriority(); prio%50 == 0 {
		np.store.logger.Infof("merged delta from node %s (priority: %d)", child, prio)
	} else {
		np.store.logger.Debugf("merged delta from node %s (priority: %d)", child, prio)
	}

	return nil
}

// updateHeads manages CRDT heads by either replacing existing heads or adding new ones
func (np *NodeProcessor) updateHeads(ctx context.Context, root cid.Cid, rootPrio uint64, headsToReplace []cid.Cid) error {
	if len(headsToReplace) > 0 {
		for _, child := range headsToReplace {
			np.store.logger.Debugf("replacing head %s -> %s", child, root)
			if err := np.store.heads.Replace(ctx, child, root, head{height: rootPrio}); err != nil {
				return fmt.Errorf("error replacing head: %s->%s: %w", child, root, err)
			}
		}
	} else {
		np.store.logger.Debugf("adding new head %s", root)
		if err := np.store.heads.Add(ctx, root, head{height: rootPrio}); err != nil {
			return fmt.Errorf("error adding head %s: %w", root, err)
		}
	}
	return nil
}
