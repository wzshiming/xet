package main

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/wzshiming/xet/hf"
)

func newUploadCmd(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upload",
		Short: "Upload files through CAS or Hugging Face XET tokens",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(newUploadCASCmd(out), newUploadHFCmd(out))
	return cmd
}

func newUploadCASCmd(out io.Writer) *cobra.Command {
	var (
		baseURL   string
		token     string
		namespace string
	)

	cmd := &cobra.Command{
		Use:   "cas <file>",
		Short: "Upload a file using the native CAS API",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return executeUpload(cmd.Context(), args[0], baseURL, token, namespace, out)
		},
	}

	cmd.Flags().StringVar(&baseURL, "url", defaultHFCASURL, "CAS server URL")
	cmd.Flags().StringVar(&token, "token", "", "CAS token")
	cmd.Flags().StringVar(&namespace, "namespace", "default", "Storage namespace")
	return cmd
}

func newUploadHFCmd(out io.Writer) *cobra.Command {
	var (
		hfRepo     string
		hfToken    string
		hfEndpoint string
		hfRepoType string
		hfRevision string
		namespace  string
	)

	cmd := &cobra.Command{
		Use:   "hf <file>",
		Short: "Upload a file using Hugging Face xet-write-token API",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if hfRepo == "" {
				return fmt.Errorf("--repo is required")
			}
			if hfToken == "" {
				return fmt.Errorf("--token is required")
			}

			hfInfo, err := hf.ResolveXETWriteToken(cmd.Context(), hfRepo, hfToken, hf.UploadOptions{
				Endpoint: hfEndpoint,
				RepoType: hfRepoType,
				Revision: hfRevision,
			})
			if err != nil {
				return fmt.Errorf("resolve Hugging Face upload target: %w", err)
			}
			if _, err := fmt.Fprintf(out, "%s Resolved Hugging Face upload target: %s/%s@%s\n", args[0], hfInfo.RepoType, hfInfo.RepoID, hfInfo.Revision); err != nil {
				return err
			}

			return executeUpload(cmd.Context(), args[0], hfInfo.BaseURL, hfInfo.Token, namespace, out)
		},
	}

	cmd.Flags().StringVar(&hfRepo, "repo", "", "Hugging Face repo ID or repo URL")
	cmd.Flags().StringVar(&hfToken, "token", "", "Hugging Face access token")
	cmd.Flags().StringVar(&hfEndpoint, "endpoint", defaultHFEndpoint, "Hugging Face Hub endpoint override")
	cmd.Flags().StringVar(&hfRepoType, "repo-type", "model", "Hugging Face repo type: model, dataset, or space")
	cmd.Flags().StringVar(&hfRevision, "revision", "main", "Hugging Face revision")
	cmd.Flags().StringVar(&namespace, "namespace", "default", "Storage namespace")
	return cmd
}
