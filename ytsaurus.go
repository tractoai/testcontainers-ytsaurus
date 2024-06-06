package ytsaurus

import (
	"context"
	"fmt"
	"net"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

const (
	defaultImage   = "ytsaurus/local:stable"
	containerPort  = "80/tcp"
	SuperuserToken = "password"
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

// NewClient creates a new YT client logged-in as a superuser.
func (y *YTsaurusContainer) NewClient(ctx context.Context) (yt.Client, error) {
	return y.NewUserClient(ctx, SuperuserToken)
}

// NewUserClient creates a new YT client connected to the YTsaurus container.
func (y *YTsaurusContainer) NewUserClient(ctx context.Context, token string) (yt.Client, error) {
	host, err := y.ConnectionHost(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection host: %w", err)
	}

	client, err := ythttp.NewClient(&yt.Config{
		Proxy: host,
		Credentials: &yt.TokenCredentials{
			Token: token,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create YT client: %w", err)
	}
	return client, nil
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
