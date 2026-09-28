// Package hf is the xet client for Hugging Face hub repositories.
package hf

import (
	"fmt"
	"net/url"
	"strings"
)

// Repo identifies a Hugging Face repository revision; zero fields default to https://huggingface.co, model and main.
type Repo struct {
	Endpoint string
	RepoType string
	RepoID   string
	Revision string
}

// CommitURL returns the commit endpoint of repo's revision, {endpoint}/api/{type}s/{repo}/commit/{revision}.
func (r Repo) CommitURL() string {
	return r.apiURL("commit")
}

// ResolveURL returns the download URL of path at repo's revision, {endpoint}/[{type}s/]{repo}/resolve/{revision}/{path}, escaped like huggingface_hub does.
func (r Repo) ResolveURL(path string) string {
	r = r.normalized()
	prefix := ""
	if r.RepoType != "model" {
		prefix = r.RepoType + "s/"
	}
	segs := strings.Split(path, "/")
	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}
	return r.Endpoint + "/" + prefix + r.RepoID + "/resolve/" + url.PathEscape(r.Revision) + "/" + strings.Join(segs, "/")
}

// ParseResolveURL splits a hub download URL, {endpoint}/[{type}s/]{repo}/resolve/{revision}/{path}, into its repository and unescaped path: the inverse of Repo.ResolveURL.
func ParseResolveURL(rawURL string) (Repo, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return Repo{}, "", fmt.Errorf("invalid resolve URL %q", rawURL)
	}
	repo, rest, ok := strings.Cut(strings.TrimPrefix(u.EscapedPath(), "/"), "/resolve/")
	rev, path, _ := strings.Cut(rest, "/")
	if !ok || repo == "" || rev == "" || path == "" {
		return Repo{}, "", fmt.Errorf("resolve URL %q: want {endpoint}/{repository}/resolve/{revision}/{path}", rawURL)
	}
	r := Repo{Endpoint: u.Scheme + "://" + u.Host, RepoType: "model", RepoID: repo, Revision: rev}
	if typ, id, ok := strings.Cut(repo, "/"); ok && (typ == "datasets" || typ == "spaces" || typ == "kernels") {
		r.RepoType, r.RepoID = typ, id
	}
	for _, s := range []*string{&r.RepoID, &r.Revision, &path} {
		if *s, err = url.PathUnescape(*s); err != nil {
			return Repo{}, "", fmt.Errorf("resolve URL %q: %w", rawURL, err)
		}
	}
	return r, path, nil
}

// apiURL returns the endpoint of the given kind, such as preupload, at repo's revision: {endpoint}/api/{type}s/{repo}/{kind}/{revision}.
func (r Repo) apiURL(kind string) string {
	r = r.normalized()
	return fmt.Sprintf("%s/api/%ss/%s/%s/%s", r.Endpoint, r.RepoType, r.RepoID, kind, url.PathEscape(r.Revision))
}

// normalized returns r with its defaults filled in and its endpoint, type and id spelled the way the hub's API paths take them.
func (r Repo) normalized() Repo {
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
