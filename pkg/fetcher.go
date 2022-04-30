package file

import (
	"context"
	"github.com/containerd/containerd/remotes"
	"github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/zeebo/errs"
	"io"
	"storj.io/uplink"
)

type Fetcher struct {
	bucket string
	access *uplink.Access
}

func (f Fetcher) Fetch(ctx context.Context, desc v1.Descriptor) (io.ReadCloser, error) {
	project, err := uplink.OpenProject(ctx, f.access)
	if err != nil {
		return nil, err
	}
	defer project.Close()

	download, err := project.DownloadObject(ctx, f.bucket, desc.Digest.String(), nil)
	if err != nil {
		return nil, err
	}
	return FinalReader{project: project, reader: download}, nil
}

var _ remotes.Fetcher = &Fetcher{}

type FinalReader struct {
	project *uplink.Project
	reader  *uplink.Download
}

func (f FinalReader) Read(p []byte) (n int, err error) {
	return f.reader.Read(p)
}

func (f FinalReader) Close() error {
	return errs.Combine(f.reader.Close(), f.project.Close())
}

var _ io.ReadCloser = FinalReader{}
