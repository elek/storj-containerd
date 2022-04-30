package file

import (
	"context"
	"encoding/json"
	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/remotes"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go/v1"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"
)

type Pusher struct {
	Ref string
}

func (p Pusher) Push(ctx context.Context, d v1.Descriptor) (content.Writer, error) {
	out, err := os.Create(path.Join("/tmp/repo", d.Digest.String()))
	if err != nil {
		return nil, err
	}
	//jsonDesc, err := json.Marshal(d)
	//if err != nil {
	//	return nil, err
	//}
	//err = ioutil.WriteFile(path.Join("/tmp/repo", p.Ref), jsonDesc, 0755)
	//if err != nil {
	//	return nil, err
	//}
	return Writer{
		ref: p.Ref,
		status: content.Status{
			StartedAt: time.Now(),
		},
		descriptor: d,
		writer:     out,
	}, nil
}

var _ remotes.Pusher = &Pusher{}

type Writer struct {
	ref        string
	descriptor v1.Descriptor
	writer     *os.File
	status     content.Status
}

func (w Writer) Write(p []byte) (n int, err error) {
	written, err := w.writer.Write(p)
	w.status.Offset = w.status.Offset + int64(written)
	return written, err
}

func (w Writer) Close() error {
	return w.writer.Close()
}

func (w Writer) Digest() digest.Digest {
	return w.descriptor.Digest
}

func (w Writer) Commit(ctx context.Context, size int64, expected digest.Digest, opts ...content.Opt) error {
	content, err := json.Marshal(w.descriptor)
	if err != nil {
		return err
	}
	name := strings.Split(w.ref, "@")[0]
	_ = os.MkdirAll(path.Dir(path.Join("/tmp/repo", name)), 0755)
	return ioutil.WriteFile(path.Join("/tmp/repo", name), content, 0755)
}

func (w Writer) Status() (content.Status, error) {
	return content.Status{}, nil
}

func (w Writer) Truncate(size int64) error {
	panic("implement me")
}

var _ content.Writer = &Writer{}
