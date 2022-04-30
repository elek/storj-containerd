package file

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/remotes"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/zeebo/errs"
	"storj.io/uplink"
	"strings"
	"time"
)

type Pusher struct {
	Ref    string
	bucket string
	access *uplink.Access
}

func (p Pusher) Push(ctx context.Context, d v1.Descriptor) (content.Writer, error) {
	project, err := uplink.OpenProject(ctx, p.access)
	if err != nil {
		return nil, err
	}
	defer project.Close()

	upload, err := project.UploadObject(ctx, p.bucket, d.Digest.String(), nil)
	if err != nil {
		return nil, err
	}

	return Writer{
		bucket:  p.bucket,
		ref:     p.Ref,
		project: project,
		upload:  upload,
		status: content.Status{
			StartedAt: time.Now(),
		},
		descriptor: d,
	}, nil
}

var _ remotes.Pusher = &Pusher{}

type Writer struct {
	ref        string
	project    *uplink.Project
	descriptor v1.Descriptor
	status     content.Status
	upload     *uplink.Upload
	bucket     string
}

func (w Writer) Write(p []byte) (n int, err error) {
	written, err := w.upload.Write(p)
	w.status.Offset += int64(written)
	return written, err
}

func (w Writer) Close() error {
	if w.upload != nil {
		_ = w.upload.Abort()
	}
	return w.project.Close()
}

func (w Writer) Digest() digest.Digest {
	return w.descriptor.Digest
}

func (w Writer) Commit(ctx context.Context, size int64, expected digest.Digest, opts ...content.Opt) error {
	err := w.upload.Commit()
	if err != nil {
		return err
	}
	w.upload = nil
	fmt.Println(w.descriptor.MediaType)
	if w.descriptor.MediaType == v1.MediaTypeImageManifest || w.descriptor.MediaType == "application/vnd.docker.distribution.manifest.v2+json" {
		descriptorJson, err := json.Marshal(w.descriptor)
		if err != nil {
			return err
		}

		name := normalizeRefName(w.ref)

		descUpload, err := w.project.UploadObject(ctx, w.bucket, name, nil)
		if err != nil {
			return err
		}
		written, err := descUpload.Write(descriptorJson)
		if err != nil {
			return err
		}
		fmt.Println(written)
		err = descUpload.Commit()
		if err != nil {
			return errs.Wrap(err)
		}
	}
	return nil
}

func normalizeRefName(ref string) string {
	name := strings.Split(ref, "@")[0]
	parts := strings.Split(name, "/")
	name = parts[len(parts)-1]
	return name
}

func (w Writer) Status() (content.Status, error) {
	return w.status, nil
}

func (w Writer) Truncate(size int64) error {
	panic("implement me")
}

var _ content.Writer = &Writer{}
