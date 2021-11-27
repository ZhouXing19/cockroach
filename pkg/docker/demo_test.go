package docker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"golang.org/x/net/context"
)

const (
	imageName   = "cockroachdb/jane_cockroach:latest"
	logFile     = "./myLog20211126.txt"
	fifoTImeout = 20
	fifoFile    = "new_fifo111"
)

type dockerNode struct {
	cli    client.APIClient
	contId string
}

func cleanUp() error {
	_, err := exec.Command("rm", "-rf", "./cockroach-data", "&&", "rm", logFile).Output()
	if err != nil {
		return fmt.Errorf("cannot cleanup: %v", err)
	}
	return nil
}

// Logs outputs the containers logs to the given io.Writer.
func (dn *dockerNode) showLogs(ctx context.Context) error {

	cmdLog, err := os.Create(logFile)
	out := io.MultiWriter(cmdLog, os.Stderr)

	rc, err := dn.cli.ContainerLogs(ctx, dn.contId, types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return err
	}
	defer rc.Close()
	// The docker log output is not quite plaintext: each line has a
	// prefix consisting of one byte file descriptor (stdout vs stderr),
	// three bytes padding, four byte length. We could use this to
	// disentangle stdout and stderr if we wanted to output them into
	// separate streams, but we don't really care.
	for {
		var header uint64
		if err := binary.Read(rc, binary.BigEndian, &header); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := header & math.MaxUint32
		if _, err := io.CopyN(out, rc, int64(size)); err != nil {
			return err
		}
	}
	return nil
}

func (dn *dockerNode) startContainer(
	ctx context.Context, envSetting []string, volSetting []string, containerName string,
) error {

	containerConfig := container.Config{
		Hostname:     "roach1",
		Image:        imageName,
		Env:          envSetting,
		ExposedPorts: nat.PortSet{"8080": struct{}{}},
		Cmd: []string{
			"start-single-node",
			"--insecure",
			fmt.Sprintf("--listening-url-file=%s", fifoFile),
		},
	}

	hostConfig := container.HostConfig{
		Binds:        volSetting,
		PortBindings: map[nat.Port][]nat.PortBinding{nat.Port("8080"): {{HostIP: "127.0.0.1", HostPort: "8080"}}},
	}

	resp, err := dn.cli.ContainerCreate(
		ctx,
		&containerConfig,
		&hostConfig,
		nil,
		nil,
		containerName,
	)
	if err != nil {
		return fmt.Errorf("cannot create container: %v", err)
	}
	fmt.Println("id:", resp.ID)

	dn.contId = resp.ID

	if err := dn.cli.ContainerStart(ctx, dn.contId, types.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("cannot start container: %v", err)
	}

	return nil
}

func (dn *dockerNode) removeAllContainer(ctx context.Context) error {
	conts, err := dn.cli.ContainerList(ctx, types.ContainerListOptions{All: true})
	if err != nil {
		return fmt.Errorf("cannot list all conts: %v", err)
	}
	for _, cont := range conts {
		err := dn.cli.ContainerRemove(ctx, cont.ID, types.ContainerRemoveOptions{Force: true})
		if err != nil {
			return fmt.Errorf("cannot remove cont %s: %v", cont.Names, err)
		}
	}
	return nil
}

type ExecResult struct {
	StdOut   string
	StdErr   string
	ExitCode int
}

func (dn *dockerNode) InspectExecResp(ctx context.Context, execId string) (ExecResult, error) {
	var execResult ExecResult
	resp, err := dn.cli.ContainerExecAttach(ctx, execId, types.ExecStartCheck{})
	if err != nil {
		return execResult, err
	}
	defer resp.Close()

	// read the output
	var outBuf, errBuf bytes.Buffer
	outputDone := make(chan error)

	go func() {
		// StdCopy demultiplexes the stream into two buffers
		_, err = stdcopy.StdCopy(&outBuf, &errBuf, resp.Reader)
		outputDone <- err
	}()

	select {
	case err := <-outputDone:
		if err != nil {
			return execResult, err
		}
		break

	case <-ctx.Done():
		return execResult, ctx.Err()
	}

	stdout, err := ioutil.ReadAll(&outBuf)
	if err != nil {
		return execResult, err
	}
	stderr, err := ioutil.ReadAll(&errBuf)
	if err != nil {
		return execResult, err
	}

	res, err := dn.cli.ContainerExecInspect(ctx, execId)
	if err != nil {
		return execResult, err
	}

	execResult.ExitCode = res.ExitCode
	execResult.StdOut = string(stdout)
	execResult.StdErr = string(stderr)
	return execResult, nil
}

func (dn *dockerNode) execCommand(ctx context.Context, cmd []string) (*ExecResult, error) {
	execId, err := dn.cli.ContainerExecCreate(ctx, dn.contId, types.ExecConfig{
		AttachStderr: true,
		AttachStdout: true,
		Tty:          true,
		Cmd:          []string{"inotifywait", "-e", "close_write", "-t", fmt.Sprint(fifoTImeout), "--include", fifoFile, "."},
	})

	if err != nil {
		return nil, fmt.Errorf("cannot create command \"%s\": %v", strings.Join(cmd[:], " "), err)
	}

	res, err := dn.InspectExecResp(ctx, execId.ID)
	if err != nil {
		return nil, fmt.Errorf("cannot execute command \"%s\": %v", strings.Join(cmd[:], " "), err)
	}

	if res.ExitCode != 0 {
		return nil, fmt.Errorf("command \"%s\" exit with code %d: %+v", strings.Join(cmd[:], " "), res.ExitCode, res)
	}

	return &res, nil

}

func (dn *dockerNode) waitTillStarted(ctx context.Context) error {
	res, err := dn.execCommand(ctx, []string{"inotifywait", "-e", "close_write", "-t", fmt.Sprint(fifoTImeout), "--include", fifoFile, "."})
	if err != nil {
		switch res.ExitCode {
		case 2:
			return fmt.Errorf("timeout for starting the server")
		default:
			return err
		}
	}

	return nil
}

func TestName(t *testing.T) {
	if err := cleanUp(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	pwd, err := os.Getwd()
	if err != nil {
		t.Fatal(fmt.Errorf("cannot get pwd: %v", err))
	}
	envSetting := []string{"COCKROACH_DATABASE=hellobee", "DOCKER_API_VERSION=1.39"}
	volSetting := []string{
		fmt.Sprintf("%s/cockroach-data/roach1:/cockroach/cockroach-data", pwd),
		fmt.Sprintf("%s/docker-entrypoint-initdb.d/:/docker-entrypoint-initdb.d", pwd),
	}

	cli, err := client.NewClientWithOpts(client.FromEnv)
	cli.NegotiateAPIVersion(ctx)

	if err != nil {
		t.Fatal(err)
	}
	dn := dockerNode{
		cli: cli,
	}

	if err := dn.removeAllContainer(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("hello", func(t *testing.T) {
		if err := dn.startContainer(ctx, envSetting, volSetting, "roach166"); err != nil {
			t.Fatal(err)
		}

		if err := dn.waitTillStarted(ctx); err != nil {
			t.Fatal(err)
		}

		// TODO (janexing): log print out to log file, not stdout.
		if err := dn.showLogs(ctx); err != nil {
			log.Warningf(ctx, "cannot showLogs: %v", err)
		}

	})
}
