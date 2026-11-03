// Package storagetest runs the backend-independent storage tests against a Backend.
package storagetest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/wzshiming/xet/storage"
)

// Backend adapts one storage implementation to the suite.
type Backend struct {
	Name string
	// New returns a fresh store.
	New func(t *testing.T) storage.Storage
	// NewWithUploadGrantTTL returns a fresh store whose upload grants expire after ttl.
	NewWithUploadGrantTTL func(t *testing.T, ttl time.Duration) storage.Storage
	// SetIndexEntry writes, overwriting any existing value, the index entry kind/name directly on the backend.
	SetIndexEntry func(t *testing.T, st storage.Storage, kind, name, shardHash string)
	// PutRawShardObject stores raw bytes as the shard object named hash, bypassing PutShard and its index writes.
	PutRawShardObject func(t *testing.T, ctx context.Context, st storage.Storage, hash string, raw []byte)
	// UploadGranted sends the length bytes of body where grant directs; nil means st.PutStagedXorb(ctx, grant.ID, body).
	UploadGranted func(t *testing.T, ctx context.Context, st storage.Storage, grant *storage.XorbUploadGrant, length int64, body io.Reader) error
}

// upload sends body under grant the way the backend's clients do.
func (b Backend) upload(t *testing.T, ctx context.Context, st storage.Storage, grant *storage.XorbUploadGrant, length int64, body io.Reader) error {
	if b.UploadGranted == nil {
		return st.PutStagedXorb(ctx, grant.ID, body)
	}
	return b.UploadGranted(t, ctx, st, grant, length, body)
}

// DirectUpload is an UploadGranted sending body as the xet client does, with exactly
// the grant's headers plus Content-Length, and mapping the status back to the
// sentinel the server's upload route answers with it.
func DirectUpload(_ *testing.T, ctx context.Context, _ storage.Storage, grant *storage.XorbUploadGrant, length int64, body io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, grant.Method, grant.URL, body)
	if err != nil {
		return err
	}
	req.ContentLength = length
	for name, value := range grant.Headers {
		req.Header.Set(name, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	var sentinel error
	switch code := resp.StatusCode; {
	case code/100 == 2:
		return nil
	case code == http.StatusPreconditionFailed:
		sentinel = storage.ErrStagedXorbExists
	case code == http.StatusNotFound:
		sentinel = storage.ErrUploadGrantNotFound
	case code == http.StatusForbidden:
		sentinel = storage.ErrUploadGrantExpired
	case code == http.StatusBadRequest:
		sentinel = storage.ErrStagedXorbMismatch
	default:
		return fmt.Errorf("direct upload answered %s", resp.Status)
	}
	return fmt.Errorf("direct upload answered %s: %w", resp.Status, sentinel)
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
	{"TestPutShardVerifiesCASChunks", testPutShardVerifiesCASChunks},
	{"TestPutShardCommitsNewFilesBesideKnownFile", testPutShardCommitsNewFilesBesideKnownFile},
	{"TestPutShardRetryRepairsMissingFileEntry", testPutShardRetryRepairsMissingFileEntry},
	{"TestPutShardCommitsMultiTermFile", testPutShardCommitsMultiTermFile},
	{"TestPutXorbRejectsWrongIdentity", testPutXorbRejectsWrongIdentity},
	{"TestPutXorbRejectsForgedChunkSizes", testPutXorbRejectsForgedChunkSizes},
	{"TestPutFileReusesStoredChunks", testPutFileReusesStoredChunks},
	{"TestPutFileEmpty", testPutFileEmpty},
	{"TestUsageCountsObjectsByKind", testUsageCountsObjectsByKind},
	{"TestXorbChunkRangesReadBack", testXorbChunkRangesReadBack},
	{"TestGrantRoundTrip", testGrantRoundTrip},
	{"TestGrantCommitExistingXorb", testGrantCommitExistingXorb},
	{"TestGrantPutLengthMismatch", testGrantPutLengthMismatch},
	{"TestGrantPutChecksumMismatch", testGrantPutChecksumMismatch},
	{"TestGrantPutRepeat", testGrantPutRepeat},
	{"TestGrantPutUnknownID", testGrantPutUnknownID},
	{"TestGrantExpired", testGrantExpired},
	{"TestGrantCommitInvalidXorb", testGrantCommitInvalidXorb},
	{"TestGrantCommitTrailingBytes", testGrantCommitTrailingBytes},
	{"TestGrantCommitRefreshesModTime", testGrantCommitRefreshesModTime},
	{"TestGrantCommitAfterExpiry", testGrantCommitAfterExpiry},
	{"TestGrantCommitUnknownID", testGrantCommitUnknownID},
	{"TestSweepReclaimsStaleStagedXorb", testSweepReclaimsStaleStagedXorb},
	{"TestSweepStepCountsStagedDeletes", testSweepStepCountsStagedDeletes},
	{"TestGrantUsageUnaffected", testGrantUsageUnaffected},
	{"TestGrantInvisibleToWalkXorbs", testGrantInvisibleToWalkXorbs},
	{"TestGrantPutInFlightHidden", testGrantPutInFlightHidden},
}
