package ytsaurus_test

import (
	"context"
	"testing"

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
			Token: ytsaurus.SuperuserToken,
		},
	})
	require.NoError(t, err)

	users := getUsers(t, ytClient)
	require.NotEmpty(t, users)
}

func TestUserLogin(t *testing.T) {
	ctx := context.Background()

	container, err := ytsaurus.RunContainer(ctx)
	require.NoError(t, err)

	// Clean up the container after the test is complete
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	ytClient, err := container.NewClient(ctx)
	require.NoError(t, err)

	newUser := "oleg"
	createUser(t, ytClient, newUser)
	token, err := ytClient.IssueToken(ctx, newUser, "", nil)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	userYtClient, err := container.NewUserClient(ctx, token)
	require.NoError(t, err)
	err = userYtClient.RemoveNode(ctx, ypath.Path("//home"), nil)
	require.Error(t, err)
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
