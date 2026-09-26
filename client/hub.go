package client

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/wzshiming/xet/auth"
)

// HubRepo identifies a Hugging Face repository revision; zero fields default to https://huggingface.co, model and main.
type HubRepo struct {
	Endpoint string
	RepoType string
	RepoID   string
	Revision string
}

// NewHubTokenProvider returns a provider fetching repo's CAS read and write tokens from the hub with hubToken.
func NewHubTokenProvider(httpClient *http.Client, repo HubRepo, hubToken string) UpstreamProvider {
	repo = repo.normalized()
	base, rev := repo.apiBase(), url.PathEscape(repo.Revision)
	return NewTokenProvider(httpClient, hubToken, map[auth.Permission]string{
		auth.Read:  base + "/xet-read-token/" + rev,
		auth.Write: base + "/xet-write-token/" + rev,
	})
}

// CommitURL returns the commit endpoint of repo's revision, {endpoint}/api/{type}s/{repo}/commit/{revision}, for Commit.
func (r HubRepo) CommitURL() string {
	r = r.normalized()
	return r.apiBase() + "/commit/" + url.PathEscape(r.Revision)
}

// apiBase is the API prefix of a normalized repository, {endpoint}/api/{type}s/{repo}.
func (r HubRepo) apiBase() string {
	return fmt.Sprintf("%s/api/%ss/%s", r.Endpoint, r.RepoType, r.RepoID)
}

// normalized returns r with its defaults filled in and its endpoint, type and id spelled the way the hub's API paths take them.
func (r HubRepo) normalized() HubRepo {
	r.Endpoint = strings.TrimRight(r.Endpoint, "/")
	if r.Endpoint == "" {
		r.Endpoint = "https://huggingface.co"
	}
	r.RepoType = normalizeRepoType(r.RepoType)
	r.RepoID = strings.Trim(r.RepoID, "/")
	if r.Revision == "" {
		r.Revision = "main"
	}
	return r
}

// normalizeRepoType maps a repository type, singular or plural in any case, to the singular of the API paths; empty and unknown types are models.
func normalizeRepoType(repoType string) string {
	switch strings.ToLower(strings.TrimSpace(repoType)) {
	case "dataset", "datasets":
		return "dataset"
	case "space", "spaces":
		return "space"
	case "kernel", "kernels":
		return "kernel"
	default:
		return "model"
	}
}
