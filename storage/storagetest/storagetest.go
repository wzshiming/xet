// Package storagetest runs the backend-independent storage tests against a Backend.
package storagetest

import (
	"context"
	"testing"

	"github.com/wzshiming/xet/storage"
)

// Backend adapts one storage implementation to the suite.
type Backend struct {
	Name string
	// New returns a fresh store; it must also implement storage.GCStore.
	New func(t *testing.T) storage.Storage
	// SetIndexEntry writes, overwriting any existing value, the index entry kind/name directly on the backend.
	SetIndexEntry func(t *testing.T, st storage.Storage, kind, name, shardHash string)
	// PutRawShardObject stores raw bytes as the shard object named hash, bypassing PutShard and its index writes.
	PutRawShardObject func(t *testing.T, ctx context.Context, st storage.Storage, hash string, raw []byte)
}

// Run runs every case as a subtest under b.Name.
func Run(t *testing.T, b Backend) {
	t.Run(b.Name, func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) { tc.run(t, b) })
		}
	})
}

var cases = []struct {
	name string
	run  func(t *testing.T, b Backend)
}{
	{"TestUnlinkRemovesFileIndexEntry", testUnlinkRemovesFileIndexEntry},
	{"TestUnlinkSHA256RemovesEntry", testUnlinkSHA256RemovesEntry},
	{"TestSweepNeedsBothUnlinks", testSweepNeedsBothUnlinks},
	{"TestSweepRemovesOrphanedObjects", testSweepRemovesOrphanedObjects},
	{"TestSweepDryRunDeletesNothing", testSweepDryRunDeletesNothing},
	{"TestSweepDryRunParity", testSweepDryRunParity},
	{"TestSweepGraceWindow", testSweepGraceWindow},
	{"TestSweepNegativeGraceSentinelSweepsFreshObjects", testSweepNegativeGraceSentinelSweepsFreshObjects},
	{"TestSweepDeletesSharedChunkEntry", testSweepDeletesSharedChunkEntry},
	{"TestSweepReportsDanglingFileEntries", testSweepReportsDanglingFileEntries},
	{"TestSweepReportsDanglingSHA256Entries", testSweepReportsDanglingSHA256Entries},
	{"TestSweepThenReuploadResurrects", testSweepThenReuploadResurrects},
	{"TestGCSweepStepSingleFlight", testGCSweepStepSingleFlight},
	{"TestSweepShieldsCommitDuringShardDeletePhase", testSweepShieldsCommitDuringShardDeletePhase},
	{"TestSweepStepDrainsInBatches", testSweepStepDrainsInBatches},
	{"TestSweepStepRecommitBetweenSteps", testSweepStepRecommitBetweenSteps},
	{"TestSweepStepDryRunIgnoresBounds", testSweepStepDryRunIgnoresBounds},
	{"TestSweepNeverTouchesShardCache", testSweepNeverTouchesShardCache},
	{"TestSweepReportsUnreadableDeadShard", testSweepReportsUnreadableDeadShard},
	{"TestSweepUnreadableShardSuppressesXorbSweep", testSweepUnreadableShardSuppressesXorbSweep},
	{"TestSweepEmptyFileZeroSHA256Cleanup", testSweepEmptyFileZeroSHA256Cleanup},
	{"TestSweepDeleteLoopAbortsOnRacingFileEntry", testSweepDeleteLoopAbortsOnRacingFileEntry},
	{"TestSweepDeleteLoopAbortsOnRacingSHA256Entry", testSweepDeleteLoopAbortsOnRacingSHA256Entry},
	{"TestSweepAbortPreservesZeroSHA256Entry", testSweepAbortPreservesZeroSHA256Entry},
	{"TestSweepAnchorSHA256LFSLifecycle", testSweepAnchorSHA256LFSLifecycle},
	{"TestSweepAnchorSHA256KeepsUnanchorableFileShards", testSweepAnchorSHA256KeepsUnanchorableFileShards},
	{"TestSweepAnchorFilesUnlinkAloneReclaims", testSweepAnchorFilesUnlinkAloneReclaims},
	{"TestSweepAnchorFilesDeletesSharedSHA256Entry", testSweepAnchorFilesDeletesSharedSHA256Entry},
	{"TestSweepAnchorFilesSkipsSHAWalk", testSweepAnchorFilesSkipsSHAWalk},
	{"TestSweepAnchorSHA256AbortsOnRacingSHA256Entry", testSweepAnchorSHA256AbortsOnRacingSHA256Entry},
	{"TestSweepUnknownAnchorFails", testSweepUnknownAnchorFails},
	{"TestSweepAnchorBothUnchanged", testSweepAnchorBothUnchanged},
	{"TestSweepStepUnreadableShardCannotLivelock", testSweepStepUnreadableShardCannotLivelock},
	{"TestSweepStepSparedUnanchorableShardCannotLivelock", testSweepStepSparedUnanchorableShardCannotLivelock},
	{"TestSweepStepExhaustedAtShardDrainSkipsXorbPhase", testSweepStepExhaustedAtShardDrainSkipsXorbPhase},
	{"TestSweepCanceledContextFailsBeforeWork", testSweepCanceledContextFailsBeforeWork},
	{"TestListFilesGroupsBySHA256", testListFilesGroupsBySHA256},
	{"TestListFilesMarksDanglingEntries", testListFilesMarksDanglingEntries},
	{"TestListFilesToleratesVanishedXorb", testListFilesToleratesVanishedXorb},
	{"TestListFilesComputesUniqueAndShared", testListFilesComputesUniqueAndShared},
	{"TestListFilesMarksInvalidChunkMetadata", testListFilesMarksInvalidChunkMetadata},
	{"TestPutShardVerifiesFileHash", testPutShardVerifiesFileHash},
}
