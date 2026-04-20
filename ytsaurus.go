package ytsaurus

import (
	"context"
	"fmt"
	"net"
	"path/filepath"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

const (
	defaultImage    = "ghcr.io/ytsaurus/local-nightly:dev-2024-10-16-50e2ea53cfec3c9973e5b065f839e05a73506945"
	containerPort   = "80/tcp"
	DefaultUser     = "admin"
	DefaultPassword = "password"
	DefaultToken    = "password"

	// localCypressMountPath is where the YTsaurus docker image expects
	// the host-provided local cypress tree to be mounted (see yt/docker/local/start.sh).
	localCypressMountPath = "/var/lib/yt/local-cypress"
)

// YTsaurusContainer represents the YTsaurus container type used in the module.
type YTsaurusContainer struct {
	testcontainers.Container
}

// ConnectionHost returns the host and dynamic port for accessing the YTsaurus container.
func (y *YTsaurusContainer) ConnectionHost(ctx context.Context) (string, error) {
	host, err := y.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("get host: %w", err)
	}

	mappedPort, err := y.MappedPort(ctx, containerPort)
	if err != nil {
		return "", fmt.Errorf("get mapped port: %w", err)
	}

	return fmt.Sprintf("%s:%s", host, mappedPort.Port()), nil
}

// GetProxy is an alias for ConnectionHost since `proxy` is more familiar term for in YTsaurus.
func (y *YTsaurusContainer) GetProxy(ctx context.Context) (string, error) {
	return y.ConnectionHost(ctx)
}

// Token returns the token for the YTsaurus container.
func (y *YTsaurusContainer) Token() string {
	return "password"
}

// NewClient creates a new YT client connected to the YTsaurus container.
func (y *YTsaurusContainer) NewClient(ctx context.Context) (yt.Client, error) {
	host, err := y.ConnectionHost(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection host: %w", err)
	}

	client, err := ythttp.NewClient(&yt.Config{
		Proxy: host,
		Credentials: &yt.TokenCredentials{
			Token: y.Token(),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create YT client: %w", err)
	}
	return client, nil
}

// WithAuth enables authentication on http proxies and creates `admin` user with password and token `password`.
func WithAuth() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Cmd = append(
			req.Cmd,
			"--native-client-supported", // required by yt_python for auth setup
			"--enable-auth",
			"--create-admin-user",
		)
		return nil
	}
}

// WithLocalCypressDir bind-mounts hostPath into the container at the path the
// YTsaurus image expects (/var/lib/yt/local-cypress) so that files there are
// imported into Cypress on startup. Also enables --wait-tablet-cell-initialization
// so dynamic tables in the tree can be mounted and populated.
//
// See yt/python/yt/environment/local_cypress.py for the format of the tree
// (file + .meta sibling; subdirectories with .meta become map_nodes).
func WithLocalCypressDir(hostPath string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		absPath, err := filepath.Abs(hostPath)
		if err != nil {
			return fmt.Errorf("resolve local cypress dir %q: %w", hostPath, err)
		}

		prev := req.HostConfigModifier
		req.HostConfigModifier = func(hc *container.HostConfig) {
			if prev != nil {
				prev(hc)
			}
			hc.Mounts = append(hc.Mounts, mount.Mount{
				Type:     mount.TypeBind,
				Source:   absPath,
				Target:   localCypressMountPath,
				ReadOnly: true,
			})
		}

		req.Cmd = append(req.Cmd, "--wait-tablet-cell-initialization")
		return nil
	}
}

// RunContainer creates and starts an instance of the YTsaurus container.
func RunContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*YTsaurusContainer, error) {
	randomPort, err := getFreePort()
	if err != nil {
		return nil, fmt.Errorf("get random free port: %w", err)
	}

	req := testcontainers.ContainerRequest{
		Image:        defaultImage,
		ExposedPorts: []string{fmt.Sprintf("%d:%s", randomPort, containerPort)},
		WaitingFor:   wait.ForLog("Local YT started"),
		Cmd: []string{
			"--fqdn",
			"localhost",
			"--proxy-config",
			fmt.Sprintf("{address_resolver={enable_ipv4=%%true;enable_ipv6=%%false;};coordinator={public_fqdn=\"localhost:%d\"}}", randomPort),
			"--enable-debug-logging",
		},
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	for _, opt := range opts {
		if err := opt.Customize(&genericContainerReq); err != nil {
			return nil, fmt.Errorf("customize container request: %w", err)
		}
	}

	container, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return nil, fmt.Errorf("start container: %w", err)
	}

	return &YTsaurusContainer{Container: container}, nil
}

func getFreePort() (port int, err error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := listener.Close(); closeErr != nil {
			err = fmt.Errorf("close listener: %w", err)
		}
	}()

	return listener.Addr().(*net.TCPAddr).Port, nil
}
