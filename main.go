package main

import (
	"context"
	"fmt"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/platforms"
	storj "github.com/elek/storj-registry/pkg"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/cobra"
	"log"
	"os"
	"runtime"
	"storj.io/uplink"

	"github.com/containerd/containerd"
)

func main() {
	rootCmd := cobra.Command{
		Use: "storj-registry",
	}
	address := rootCmd.PersistentFlags().String("address", "/var/run/docker/containerd/containerd.sock", "Containerd socket address")
	rootCmd.AddCommand(&cobra.Command{
		Use: "pull <image>",
		RunE: func(cmd *cobra.Command, args []string) error {
			return pull(*address, args[0])
		},
	})
	rootCmd.AddCommand(&cobra.Command{
		Use: "push <image>",
		RunE: func(cmd *cobra.Command, args []string) error {
			return push(*address, args[0])
		},
	})
	err := rootCmd.Execute()
	if err != nil {
		log.Fatalf("%++v", err)
	}
}

func pull(address string, imageName string) error {
	client, err := containerd.New(address)
	if err != nil {
		return err
	}
	defer client.Close()

	access, err := uplink.ParseAccess(os.Getenv("UPLINK_ACCESS"))
	if err != nil {
		return err
	}
	storjResolver := storj.Resolver{
		Bucket: "docker",
		Access: access,
	}

	ctx := namespaces.WithNamespace(context.Background(), "default")

	image, err := client.Pull(ctx, imageName,
		containerd.WithPullUnpack,
		containerd.WithResolver(storjResolver))
	if err != nil {
		return err
	}
	fmt.Println(image.Name())

	return nil
}

func push(address string, imageName string) error {
	client, err := containerd.New(address)
	if err != nil {
		return err
	}
	ctx := namespaces.WithNamespace(context.Background(), "default")
	img, err := client.GetImage(ctx, imageName)
	if err != nil {
		return err
	}

	descriptor, err := findDescriptor(ctx, client, img)
	if err != nil {
		return err
	}

	access, err := uplink.ParseAccess(os.Getenv("UPLINK_ACCESS"))
	if err != nil {
		return err
	}
	storjResolver := storj.Resolver{
		Bucket: "docker",
		Access: access,
	}

	err = client.Push(ctx, imageName, descriptor, containerd.WithResolver(storjResolver))

	if err != nil {
		return err
	}

	defer client.Close()
	return nil
}

func findDescriptor(ctx context.Context, client *containerd.Client, img containerd.Image) (specs.Descriptor, error) {
	manifests, err := images.Children(ctx, client.ContentStore(), img.Target())
	if err != nil {
		return specs.Descriptor{}, err
	}
	matcher := platforms.NewMatcher(specs.Platform{
		Architecture: runtime.GOARCH,
		OS:           runtime.GOOS,
	})
	for _, m := range manifests {
		if matcher.Match(*m.Platform) {
			return m, nil
		}
		fmt.Println(m.Digest)
	}
	return specs.Descriptor{}, fmt.Errorf("Descriptor couldn't be found for the current platform")
}
