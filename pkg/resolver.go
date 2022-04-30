package file

import (
	"context"
	"encoding/json"
	"github.com/containerd/containerd/remotes"
	"github.com/opencontainers/image-spec/specs-go/v1"
	"io/ioutil"
	"storj.io/uplink"
)

type Resolver struct {
	Bucket string
	Access *uplink.Access
}

func (r Resolver) Resolve(ctx context.Context, ref string) (name string, desc v1.Descriptor, err error) {
	project, err := uplink.OpenProject(ctx, r.Access)
	if err != nil {
		return name, desc, err
	}
	defer project.Close()

	download, err := project.DownloadObject(ctx, r.Bucket, normalizeRefName(ref), nil)
	if err != nil {
		return name, desc, err
	}
	defer download.Close()

	content, err := ioutil.ReadAll(download)
	if err != nil {
		return name, desc, err
	}

	err = json.Unmarshal(content, &desc)
	if err != nil {
		return name, desc, err
	}
	name = ref
	return name, desc, err
}

func (r Resolver) Fetcher(ctx context.Context, ref string) (remotes.Fetcher, error) {
	return Fetcher{
		bucket: r.Bucket,
		access: r.Access,
	}, nil
}

func (r Resolver) Pusher(ctx context.Context, ref string) (remotes.Pusher, error) {
	return Pusher{
		Ref:    ref,
		bucket: r.Bucket,
		access: r.Access,
	}, nil
}

var _ remotes.Resolver = &Resolver{}
