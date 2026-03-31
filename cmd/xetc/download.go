package main

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/hf"
)

func newDownloadCmd(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "download",
		Short: "Download files through CAS, Hugging Face tokens, or resolve URLs",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(newDownloadCASCmd(out), newDownloadHFCmd(out), newDownloadResolveCmd(out))
	return cmd
}

func newDownloadCASCmd(out io.Writer) *cobra.Command {
	var (
		baseURL     string
		token       string
		namespace   string
		concurrency int
		resume      bool
	)

	cmd := &cobra.Command{
		Use:   "cas <hash> <file>",
		Short: "Download a file using the native CAS API",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			fileHash, err := xet.ParseHash(args[0])
			if err != nil {
				return fmt.Errorf("invalid file hash: %w", err)
			}
			return executeDownload(cmd.Context(), fileHash, args[1], baseURL, token, namespace, concurrency, resume, out)
		},
	}

	cmd.Flags().StringVar(&baseURL, "url", defaultHFCASURL, "CAS server URL")
	cmd.Flags().StringVar(&token, "token", "", "CAS token")
	cmd.Flags().StringVar(&namespace, "namespace", "default", "Storage namespace")
	cmd.Flags().IntVar(&concurrency, "concurrency", client.DefaultDownloadConcurrency, "Number of xorb ranges to prefetch concurrently")
	cmd.Flags().BoolVar(&resume, "resume", false, "Resume a partially downloaded file")
	return cmd
}

func newDownloadHFCmd(out io.Writer) *cobra.Command {
	var (
		hfRepo      string
		hfToken     string
		hfEndpoint  string
		hfRepoType  string
		hfRevision  string
		namespace   string
		concurrency int
		resume      bool
	)

	cmd := &cobra.Command{
		Use:   "hf <hash> <file>",
		Short: "Download a file using Hugging Face xet-read-token API",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if hfRepo == "" {
				return fmt.Errorf("--repo is required")
			}
			if hfToken == "" {
				return fmt.Errorf("--token is required")
			}

			fileHash, err := xet.ParseHash(args[0])
			if err != nil {
				return fmt.Errorf("invalid file hash: %w", err)
			}

			hfInfo, err := hf.ResolveXETReadToken(cmd.Context(), hfRepo, hfToken, hf.UploadOptions{
				Endpoint: hfEndpoint,
				RepoType: hfRepoType,
				Revision: hfRevision,
			})
			if err != nil {
				return fmt.Errorf("resolve Hugging Face download target: %w", err)
			}
			if _, err := fmt.Fprintf(out, "%s Resolved Hugging Face download target: %s/%s@%s\n", args[1], hfInfo.RepoType, hfInfo.RepoID, hfInfo.Revision); err != nil {
				return err
			}

			return executeDownload(cmd.Context(), fileHash, args[1], hfInfo.BaseURL, hfInfo.Token, namespace, concurrency, resume, out)
		},
	}

	cmd.Flags().StringVar(&hfRepo, "repo", "", "Hugging Face repo ID or repo URL")
	cmd.Flags().StringVar(&hfToken, "token", "", "Hugging Face access token")
	cmd.Flags().StringVar(&hfEndpoint, "endpoint", defaultHFEndpoint, "Hugging Face Hub endpoint override")
	cmd.Flags().StringVar(&hfRepoType, "repo-type", "model", "Hugging Face repo type: model, dataset, or space")
	cmd.Flags().StringVar(&hfRevision, "revision", "main", "Hugging Face revision")
	cmd.Flags().StringVar(&namespace, "namespace", "default", "Storage namespace")
	cmd.Flags().IntVar(&concurrency, "concurrency", client.DefaultDownloadConcurrency, "Number of xorb ranges to prefetch concurrently")
	cmd.Flags().BoolVar(&resume, "resume", false, "Resume a partially downloaded file")
	return cmd
}

func newDownloadResolveCmd(out io.Writer) *cobra.Command {
	var (
		concurrency int
		resume      bool
	)

	cmd := &cobra.Command{
		Use:   "resolve <resolve-url> <file>",
		Short: "Resolve a Hugging Face URL and download through CAS",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			hfInfo, err := hf.ResolveDownload(cmd.Context(), nil, args[0])
			if err != nil {
				return fmt.Errorf("resolve download target: %w", err)
			}
			if _, err := fmt.Fprintf(out, "%s Resolved Hugging Face file hash: %s\n", args[1], hfInfo.Hash.String()); err != nil {
				return err
			}
			return executeDownload(cmd.Context(), hfInfo.Hash, args[1], hfInfo.BaseURL, hfInfo.Token, "default", concurrency, resume, out)
		},
	}
	cmd.Flags().IntVar(&concurrency, "concurrency", client.DefaultDownloadConcurrency, "Number of xorb ranges to prefetch concurrently")
	cmd.Flags().BoolVar(&resume, "resume", false, "Resume a partially downloaded file")
	return cmd
}
