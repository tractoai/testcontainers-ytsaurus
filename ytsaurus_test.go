package ytsaurus_test

import (
	"context"
	"testing"

	"go.ytsaurus.tech/yt/go/yterrors"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"

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

func TestProxy(t *testing.T) {
	ctx := context.Background()
	container, err := ytsaurus.RunContainer(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	proxy, err := container.GetProxy(ctx)
	require.NoError(t, err)
	ytClient, err := ythttp.NewClient(&yt.Config{
		Proxy: proxy,
		Credentials: &yt.TokenCredentials{
			Token: container.Token(),
		},
	})
	require.NoError(t, err)

	users := getUsers(t, ytClient)
	require.NotEmpty(t, users)
}

func TestLocalYtsaurusWithAuth(t *testing.T) {
	ctx := context.Background()
	container, err := ytsaurus.RunContainer(ctx, ytsaurus.WithAuth())
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	proxy, err := container.GetProxy(ctx)
	require.NoError(t, err)

	ytClient, err := ythttp.NewClient(&yt.Config{
		Proxy: proxy,
		Credentials: &yt.TokenCredentials{
			Token: container.Token(),
		},
	})
	require.NoError(t, err)

	var rootMapNode []string
	err = ytClient.ListNode(ctx, ypath.Path("/"), &rootMapNode, nil)
	require.NoError(t, err)
	require.NotEmpty(t, rootMapNode)

	crookedYtClient, err := ythttp.NewClient(&yt.Config{
		Proxy: proxy,
		Credentials: &yt.TokenCredentials{
			Token: "not-a-valid-token",
		},
	})
	require.NoError(t, err)

	err = crookedYtClient.ListNode(ctx, ypath.Path("/"), &rootMapNode, nil)
	require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeAuthenticationError))
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
