package ytsaurus_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"

	ytsaurus "github.com/tractoai/testcontainers-ytsaurus"
)

func TestLocalYtsaurus(t *testing.T) {
	ctx := context.Background()

	container, err := ytsaurus.RunContainer(ctx, testcontainers.WithImage("ytsaurus/local:stable"))
	require.NoError(t, err)

	// Clean up the container after the test is complete
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	ytClient, err := container.NewClient(ctx)
	require.NoError(t, err)

	newUserName := "oleg"
	usernamesBefore := getUsers(t, ytClient)
	require.NotContains(t, usernamesBefore, newUserName)
	createUser(t, ytClient, newUserName)
	usernamesAfter := getUsers(t, ytClient)
	require.Contains(t, usernamesAfter, newUserName)
}

func getUsers(t *testing.T, client yt.Client) []string {
	var usernames []string
	err := client.ListNode(context.Background(), ypath.Path("//sys/users"), &usernames, nil)
	require.NoError(t, err)
	return usernames
}

func createUser(t *testing.T, client yt.Client, name string) {
	_, err := client.CreateObject(
		context.Background(),
		yt.NodeUser,
		&yt.CreateObjectOptions{
			Attributes: map[string]any{
				"name": name,
			},
		},
	)
	require.NoError(t, err)
}
