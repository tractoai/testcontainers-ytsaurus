package ytsaurus_test

import (
	"context"
	"flag"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"

	ytsaurus "github.com/tractoai/testcontainers-ytsaurus"
)

var localCypressDir = flag.String(
	"local-cypress-dir",
	"testdata/local_cypress_tree_dynamic",
	"host path to a local_cypress_dir tree to bind-mount into the container",
)

type dynRow struct {
	Key   int64  `yson:"key"`
	Value string `yson:"value"`
}

func TestLocalCypressDynamicTable(t *testing.T) {
	ctx := context.Background()

	container, err := ytsaurus.RunContainer(ctx, ytsaurus.WithLocalCypressDir(*localCypressDir))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	client, err := container.NewClient(ctx)
	require.NoError(t, err)

	tablePath := ypath.Path("//dyn_table")

	dyn := true
	require.NoError(t, client.AlterTable(ctx, tablePath, &yt.AlterTableOptions{Dynamic: &dyn}))
	require.NoError(t, client.MountTable(ctx, tablePath, nil))
	waitTabletState(t, ctx, client, tablePath, "mounted")

	var isDynamic bool
	require.NoError(t, client.GetNode(ctx, tablePath.Attr("dynamic"), &isDynamic, nil))
	require.True(t, isDynamic, "//dyn_table/@dynamic should be true")

	var myattr int64
	require.NoError(t, client.GetNode(ctx, tablePath.Attr("myattr"), &myattr, nil))
	require.Equal(t, int64(7), myattr)

	reader, err := client.SelectRows(ctx, "* from [//dyn_table]", nil)
	require.NoError(t, err)

	var rows []dynRow
	for reader.Next() {
		var r dynRow
		require.NoError(t, reader.Scan(&r))
		rows = append(rows, r)
	}
	require.NoError(t, reader.Err())
	require.NoError(t, reader.Close())

	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	require.Equal(t, []dynRow{
		{Key: 1, Value: "one"},
		{Key: 2, Value: "two"},
		{Key: 3, Value: "three"},
	}, rows)
	t.Logf("select_rows(* from //dyn_table): %+v", rows)

	type dynKey struct {
		Key int64 `yson:"key"`
	}
	lookupReader, err := client.LookupRows(
		ctx,
		ypath.Path("//dyn_table"),
		[]any{dynKey{Key: 2}},
		nil,
	)
	require.NoError(t, err)

	var looked []dynRow
	for lookupReader.Next() {
		var r dynRow
		require.NoError(t, lookupReader.Scan(&r))
		looked = append(looked, r)
	}
	require.NoError(t, lookupReader.Err())
	require.NoError(t, lookupReader.Close())

	require.Equal(t, []dynRow{{Key: 2, Value: "two"}}, looked)
	t.Logf("lookup_rows(//dyn_table, key=2): %+v", looked)
}

func waitTabletState(t *testing.T, ctx context.Context, client yt.Client, path ypath.Path, want string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var state string
		require.NoError(t, client.GetNode(ctx, path.Attr("tablet_state"), &state, nil))
		if state == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("tablet_state of %s is %q, want %q (timed out)", path, state, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
