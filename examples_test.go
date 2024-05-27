package ytsaurus_test

import (
	"context"
	"fmt"

	ytsaurus "github.com/nebius/testcontainers-ytsaurus"
	"github.com/testcontainers/testcontainers-go"
	"go.ytsaurus.tech/yt/go/ypath"
)

func ExampleRunContainer() {
	ctx := context.Background()

	// Start a YTsaurus container
	container, err := ytsaurus.RunContainer(ctx, testcontainers.WithImage("ytsaurus/local:stable"))
	if err != nil {
		fmt.Printf("Error starting container: %s\n", err)
		return
	}

	// Clean up the container after the example is complete
	defer func() {
		if err := container.Terminate(ctx); err != nil {
			fmt.Printf("Error terminating container: %s\n", err)
		}
	}()

	// Create a YT client
	ytClient, err := container.NewClient(ctx)
	if err != nil {
		fmt.Printf("Error creating YT client: %s\n", err)
		return
	}

	// List root
	var owner string
	err = ytClient.GetNode(ctx, ypath.Path("//home").Attr("owner"), &owner, nil)
	if err != nil {
		fmt.Printf("Get attr: %+v", err)
		return
	}

	fmt.Printf("Owner: %v\n", owner)

	// Output:
	// Owner: root
}
