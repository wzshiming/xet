package lfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/wzshiming/xet/cmd/xetc/internal/common"
	"github.com/wzshiming/xet/lfs"
)

func NewCommand() *cobra.Command {
	var (
		hfRepo      string
		hfToken     string
		hfEndpoint  string
		hfRepoType  string
		hfRevision  string
		namespace   string
		concurrency int
	)

	cmd := &cobra.Command{
		Use:     "lfs <file>",
		Aliases: []string{"hf"},
		Short:   "Upload a file using Hugging Face LFS batch + XET transfer",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if hfRepo == "" {
				return fmt.Errorf("--repo is required")
			}
			if hfToken == "" {
				return fmt.Errorf("--token is required")
			}

			oid, size, err := computeFileLFSOIDAndSize(args[0])
			if err != nil {
				return fmt.Errorf("compute file LFS oid: %w", err)
			}

			lfsObj := lfs.BatchObject{OID: oid, Size: size}
			batchResults, err := lfs.ResolveOIDUpload(cmd.Context(), hfToken, lfs.Target{
				Endpoint: hfEndpoint,
				RepoType: hfRepoType,
				RepoID:   hfRepo,
				Revision: hfRevision,
			}, lfsObj)
			if err != nil {
				return fmt.Errorf("associate LFS oid with Hugging Face xet upload: %w", err)
			}
			if _, err := fmt.Fprintf(os.Stderr, "%s Associated LFS OID: %s (%d bytes)\n", args[0], oid, size); err != nil {
				return err
			}

			if len(batchResults) == 0 {
				return fmt.Errorf("no batch results returned from LFS batch API")
			}
			batchResult := batchResults[0]

			if batchResult.Upload == nil {
				if _, err := fmt.Fprintf(os.Stderr, "%s File already exists on server, skipping upload\n", args[0]); err != nil {
					return err
				}
				return nil
			}

			// Use the session-specific CAS URL and token from the batch response when available.
			casURL := batchResult.Upload.Header["X-Xet-Cas-Url"]

			casToken := batchResult.Upload.Header["X-Xet-Access-Token"]

			if err := common.ExecuteUpload(cmd.Context(), args[0], casURL, casToken, namespace, concurrency, os.Stderr); err != nil {
				return err
			}

			if batchResult.Verify != nil {
				if _, err := fmt.Fprintf(os.Stderr, "%s Verifying upload with Hub...\n", args[0]); err != nil {
					return err
				}
				if err := lfs.VerifyObject(cmd.Context(), batchResult.Verify, lfsObj); err != nil {
					return fmt.Errorf("verify LFS object: %w", err)
				}
				if _, err := fmt.Fprintf(os.Stderr, "%s LFS verify complete\n", args[0]); err != nil {
					return err
				}
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&hfRepo, "repo", "", "Hugging Face repo ID or repo URL")
	cmd.Flags().StringVar(&hfToken, "token", "", "Hugging Face access token")
	cmd.Flags().StringVar(&hfEndpoint, "endpoint", common.DefaultHFEndpoint, "Hugging Face Hub endpoint override")
	cmd.Flags().StringVar(&hfRepoType, "repo-type", "model", "Hugging Face repo type: model, dataset, or space")
	cmd.Flags().StringVar(&hfRevision, "revision", "main", "Hugging Face revision")
	cmd.Flags().StringVar(&namespace, "namespace", "default", "Storage namespace")
	cmd.Flags().IntVar(&concurrency, "concurrency", 4, "Number of upload tasks to run concurrently")
	return cmd
}

func computeFileLFSOIDAndSize(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, fmt.Errorf("hash file: %w", err)
	}

	return hex.EncodeToString(h.Sum(nil)), n, nil
}
