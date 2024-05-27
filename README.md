# YTsaurus

## Introduction

The Testcontainers module for [YTsaurus](https://ytsaurus.tech/).

## Adding this module to your project dependencies

Please run the following command to add the YTsaurus module to your Go dependencies:

```
go get github.com/nebius/testcontainers-ytsaurus
```

## Usage example

```go
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
```

## Module reference

The YTsaurus module exposes one entrypoint function to create the YTsaurus container, and this function receives two parameters:

```golang
func RunContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*YTsaurusContainer, error)
```

- `context.Context`, the Go context.
- `testcontainers.ContainerCustomizer`, a variadic argument for passing options.

### Container Options

When starting the YTsaurus container, you can pass options in a variadic way to configure it.

#### Image

If you need to set a different YTsaurus Docker image, you can use `testcontainers.WithImage` with a valid Docker image
for YTsaurus. E.g. `testcontainers.WithImage("ytsaurus/local:stable")`.
