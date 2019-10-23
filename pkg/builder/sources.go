package builder

import (
	"crypto/tls"
	"fmt"
	"github.com/nuclio/logger"
	"github.com/v3io/xcp/backends"
	"github.com/v3io/xcp/common"
	"github.com/v3io/xcp/operators"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/transport/client"
	githttp "gopkg.in/src-d/go-git.v4/plumbing/transport/http"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func GetSourceRepo(cfg *SourceConfig) (SourceRepo, error) {
	cfg.logger, _ = common.NewLogger("info")
	if !strings.Contains(cfg.Source, "://") {
		return NewFileSource(cfg)
	}

	u, err := url.Parse(cfg.Source)
	if err != nil {
		return nil, err
	}

	password, hasPassword := u.User.Password()
	if hasPassword {
		cfg.Password = password
	}
	if u.User.Username() != "" {
		cfg.User = u.User.Username()
	}

	switch strings.ToLower(u.Scheme) {
	case "git":
		return NewGitSource(u, cfg)
	case "s3", "v3io", "v3ios":
		return newXcpSource(u, cfg)
	default:
		return nil, fmt.Errorf("Unknown backend (%s) use s3, v3io or git", u.Scheme)
	}
}

type SourceConfig struct {
	Source    string
	LocalPath string
	User      string
	Password  string
	logger    logger.Logger
}

type SourceRepo interface {
	Download() error
	CodePath() string
}

func setFrom(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

type FileSource struct {
	fullpath string
}

func NewFileSource(cfg *SourceConfig) (SourceRepo, error) {
	return &FileSource{fullpath: cfg.Source}, nil
}

func (s *FileSource) CodePath() string {
	return s.fullpath
}

func (s *FileSource) Download() error {
	return nil //TBD
}

type xcpSource struct {
	cfg     *SourceConfig
	lsTask  *backends.ListDirTask
	workers int
}

func newXcpSource(u *url.URL, cfg *SourceConfig) (SourceRepo, error) {
	src, err := common.UrlParse(cfg.Source, true)
	if err != nil {
		return nil, err
	}

	newXcpSource := xcpSource{cfg: cfg, workers: 8}
	newXcpSource.lsTask = &backends.ListDirTask{
		Source:    src,
		Since:     time.Time{},
		Recursive: true,
		InclEmpty: true,
	}

	return &newXcpSource, nil
}

func (s *xcpSource) CodePath() string {
	return s.cfg.LocalPath
}

func (s *xcpSource) Download() error {
	dst, _ := common.UrlParse(s.cfg.LocalPath, true)
	err := operators.CopyDir(s.lsTask, dst, s.cfg.logger, s.workers)
	return err
}

type GitSource struct {
	cfg      *SourceConfig
	url      string
	branch   string
	subpath  string
	codePath string
}

func NewGitSource(u *url.URL, cfg *SourceConfig) (SourceRepo, error) {
	g := GitSource{url: "https://" + u.Host + u.Path, cfg: cfg}
	g.branch = u.Fragment
	ss := strings.Split(u.Fragment, ":")
	if len(ss) > 1 {
		g.branch = ss[0]
		g.subpath = ss[1]
	}
	if g.branch == "" {
		g.branch = "master"
	}
	return &g, nil
}

func (g *GitSource) CodePath() string {
	return g.codePath
}

func (g *GitSource) Download() error {
	opts := git.CloneOptions{
		URL:           g.url,
		Depth:         1,
		ReferenceName: plumbing.ReferenceName("refs/heads/" + g.branch),
		SingleBranch:  true,
		Tags:          git.NoTags,
		Progress:      os.Stdout,
	}

	if g.cfg.Password != "" {
		opts.Auth = &githttp.BasicAuth{Username: g.cfg.User, Password: g.cfg.Password}
	}
	g.codePath = filepath.Join(g.cfg.LocalPath, g.subpath)
	r, err := git.PlainClone(g.cfg.LocalPath, false, &opts)
	if err != nil {
		return err
	}
	ref, err := r.Head()
	fmt.Printf("cloned repo %s, %s\n", ref.Name(), ref.Hash())
	return err
}

// NewService initializes a new service.
func NewService() {
	httpsCli := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 300 * time.Second,
	}

	client.InstallProtocol("https", githttp.NewClient(httpsCli))
}

func SplitUrl(url string) (path, branch, subdir string) {
	ss := strings.Split(url, "#")
	if len(ss) == 0 {
		return
	}
	path = ss[0]
	branch = "master"
	if len(ss) > 1 && ss[1] != "" {
		branch = ss[1]
	}
	ss = strings.Split(branch, ":")
	if len(ss) > 1 {
		branch = ss[0]
		subdir = ss[1]
	}

	return
}
