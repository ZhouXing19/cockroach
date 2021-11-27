package docker

import (
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const (
	FifoFile                 = "new_fifo"
	buildContainerTimeout    = 300
	startContainerTimeout    = 20
	stopContainerTimeout     = 300
	cpFileToContainerTimeout = 30
)

// removeAllContainers stops and removes all existing containers.
func removeAllContainers() error {
	_, err := exec.Command(
		"sh",
		"-c",
		"docker",
		"container",
		"ls",
		"-a",
		"-q",
		"|",
		"xargs",
		"docker",
		"container",
		"rm",
		"-f",
	).Output()
	if err != nil {
		return fmt.Errorf("failed to remove all containers: %v", err)
	}
	return nil
}

// listContainers print the list of all active containers.
func listContainers() (string, error) {
	output, err := exec.Command("docker", "container", "ls").Output()
	if err != nil {
		return "", fmt.Errorf("failed tp list all active docker containers: %v",
			err)
	}
	return string(output), nil
}

// logContainer prints the logs of a container. This is a helper function the
// server fails to start a container.
func logContainer(containerId string) error {
	output, err := exec.Command("docker", "logs", containerId).Output()
	if err != nil {
		return fmt.Errorf("failed to show log of container: %v", err)
	}

	fmt.Println(string(output))
	return nil
}

// buildDockerContainer builds the docker container with containerName from a given
// docker image specified in dockerRunArgs.
func buildDockerContainer(dockerRunArgs []string, containerName string) error {
	// Must make sure that the container name is consistent with the one
	// in dockerRunArgs.
	dockerRunArgs = append(dockerRunArgs, fmt.Sprintf("--listening-url-file=%s", FifoFile))

	if len(dockerRunArgs) == 0 {
		return fmt.Errorf("args for docker run failed to be empty")
	}

	if dockerRunArgs[0] != "run" {
		return fmt.Errorf("the first arg of dockerRunArgs must be \"run\"")
	}

	output, err := exec.Command(
		"docker",
		dockerRunArgs...,
	).Output()
	if err != nil {
		return fmt.Errorf("failed to run the docker: %s", err)
	}

	containerId := strings.TrimSpace(string(output))

	cpFinished := make(chan error)

	go func() {
		_, err := exec.Command("docker", "cp", "./read_fifo.sh",
			fmt.Sprintf("%s:cockroach/", containerName)).Output()
		if err != nil {
			cpFinished <- fmt.Errorf("cannot copy read_fifo to container: %v", err)
			return
		}
		close(cpFinished)
	}()

	cpFileTimeout := time.After(cpFileToContainerTimeout * time.Second)

	select {
	case <-cpFileTimeout:
		return fmt.Errorf("copy read_fifo to container timeout")
	case err := <-cpFinished:
		if err != nil {
			return err
		}
		fmt.Println("successfully copied the file!")
	}

	timeout := time.Second * startContainerTimeout
	for start := time.Now(); ; {
		if time.Since(start) > timeout {
			if err := logContainer(containerId); err != nil {
				return fmt.Errorf("error to check log for container %s: %v",
					containerId, err)
			}
			return fmt.Errorf("timeout for container %s to start", containerId)
		}

		containerList, err := listContainers()
		if err != nil {
			return err
		}

		if strings.Contains(containerList, containerName) {
			fmt.Printf("container %s is running", containerId)
			break
		}
	}

	//time.Sleep(1 * time.Second)

	// Check if the cockroach server is started by checking if the fifo is passed
	// with the url.
	type outputStruct struct {
		output string
		err    error
	}

	finished := make(chan outputStruct)
	go func() {
		output, err = exec.Command(
			"sh",
			"-c",
			"docker",
			"exec",
			"-i",
			containerName,
			"./read_fifo.sh",
			FifoFile,
			"30",
		).Output()
		finished <- outputStruct{string(output), err}
	}()
	timeout = time.Second * buildContainerTimeout
	select {
	case <-time.After(timeout):
		return fmt.Errorf("timeout for building container")
	case commandOutput := <-finished:
		if commandOutput.err != nil {
			return fmt.Errorf("failed reading fifo file: %v", err)
		}
		fmt.Printf("successfully built container, "+
			"with cockroach single node running on %s \n", output)

		return nil
	}
}

// executeSqlQuery executes the sql query in a given container,
// and returns the output of this query and an error.
func executeSqlQuery(
	query string, containerName string, cockroachSqlArgs []string,
) (string, error) {
	dockerExecCommand := append([]string{
		"exec",
		"-i",
		containerName,
		"./cockroach",
		"sql",
		"-e",
		fmt.Sprintf("%s;", query),
	}, cockroachSqlArgs...)

	cmd := exec.Command(
		"docker",
		dockerExecCommand...,
	)
	stdout, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to execute sql query %s in docker container"+
			" %s"+
			": %v"+
			"", query, containerName, err)
	}
	// Return the output of sql query.
	return string(stdout), nil
}

// removeContainer removes a container,
// and returns an error if it failed to be removed or the removal times out.
func removeContainer(containerName string) error {
	cmd := exec.Command(
		"docker",
		"rm",
		"-f",
		containerName,
	)
	_, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to remove a container %s: %v",
			containerName,
			err)
	}

	var output []byte
	timeout := time.Second * stopContainerTimeout
	for start := time.Now(); ; {
		if time.Since(start) > timeout {
			return fmt.Errorf("timeout for removing container %s", containerName)
		}
		// Check if the container is truly removed.
		output, err = exec.Command(
			"sh",
			"-c",
			"docker",
			"container",
			"ls",
			"-a",
			"|",
			"grep",
			containerName,
		).Output()
		if len(output) == 0 {
			fmt.Printf("docker container %s is fully stopped \n", containerName)
			break
		}
	}
	return nil
}
