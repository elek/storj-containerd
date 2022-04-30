package file

import (
	"context"
	"encoding/json"
	"github.com/containerd/containerd/remotes"
	"github.com/opencontainers/image-spec/specs-go/v1"
	"io/ioutil"
	"os"
	"path"
)

type Resolver struct {
}

func (r Resolver) Resolve(ctx context.Context, ref string) (name string, desc v1.Descriptor, err error) {
	refFile := path.Join("/tmp/repo/" + ref)
	_, err = os.Stat(refFile)
	if err != nil {
		return name, desc, err
	}

	content, err := ioutil.ReadFile(refFile)
	if err != nil {
		return name, desc, err
	}
	err = json.Unmarshal(content, &desc)
	if err != nil {
		return name, desc, err
	}
	name = ref
	return "asd", desc, err
}

func (r Resolver) Fetcher(ctx context.Context, ref string) (remotes.Fetcher, error) {
	return Fetcher{}, nil
}

func (r Resolver) Pusher(ctx context.Context, ref string) (remotes.Pusher, error) {
	return Pusher{
		Ref: ref,
	}, nil
}

var _ remotes.Resolver = &Resolver{}
