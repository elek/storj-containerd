package file

import (
	"context"
	"github.com/containerd/containerd/remotes"
	"github.com/opencontainers/image-spec/specs-go/v1"
	"io"
	"os"
	"path"
)

type Fetcher struct {
}

func (f Fetcher) Fetch(ctx context.Context, desc v1.Descriptor) (io.ReadCloser, error) {
	return os.Open(path.Join("/tmp/repo", desc.Digest.String()))
}

var _ remotes.Fetcher = &Fetcher{}
