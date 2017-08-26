package bootstrap

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/buildkite/agent/bootstrap/shell"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/nightlyone/lockfile"
)

type knownHosts struct {
	*lockfile.Lockfile
	sh   *shell.Shell
	Path string
}

func findKnownHosts(sh *shell.Shell) (*knownHosts, error) {
	userHomePath, err := homedir.Dir()
	if err != nil {
		warningf("Could not find the current users home directory (%s)", err)
		return nil, err
	}

	// Construct paths to the known_hosts file
	sshDirectory := filepath.Join(userHomePath, ".ssh")
	knownHostPath := filepath.Join(sshDirectory, "known_hosts")

	// Ensure a directory exists and known_host file exist
	if !fileExists(knownHostPath) {
		os.MkdirAll(sshDirectory, 0755)
		ioutil.WriteFile(knownHostPath, []byte(""), 0644)
	}

	lockFile := filepath.Join(sshDirectory, "known_hosts.lock")

	// Create a lock on the known_host file so other agents don't try and
	// change it at the same time
	knownHostLock, err := acquireLockWithTimeout(lockFile, time.Second*30)
	if err != nil {
		warningf("Could not acquire a lock on `%s`: %v", lockFile, err)
		return nil, err
	}

	return &knownHosts{knownHostLock, sh, knownHostPath}, nil
}

func (kh *knownHosts) Add(host string) error {
	// Try and open the existing hostfile in (append_only) mode
	f, err := os.OpenFile(kh.Path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		warningf("Could not open \"%s\" for reading (%s)", kh.Path, err)
		return err
	}
	defer f.Close()

	sshToolsDir, err := findSSHToolsDir(kh.sh)
	if err != nil {
		return err
	}

	// Grab the generated keys for the repo host
	p, err := kh.sh.Subprocess(filepath.Join(sshToolsDir, "ssh-keygen"), "-f", kh.Path, "-F", host)
	if err != nil {
		exitf("%v", err)
	}

	keygenOutput, err := p.RunAndOutput()
	if err != nil {
		warningf("Could not perform `ssh-keygen` (%s)", err)
		return err
	}

	// If the keygen output already contains the host, we can skip!
	if strings.Contains(keygenOutput, host) {
		commentf("Host \"%s\" already in list of known hosts at \"%s\"", host, kh.Path)
		return nil
	}

	// Scan the key and then write it to the known_host file
	p, err = kh.sh.Subprocess(filepath.Join(sshToolsDir, "ssh-keyscan"), host)
	if err != nil {
		exitf("%v", err)
	}

	keyscanOutput, err := p.RunAndOutput()
	if err != nil {
		warningf("Could not perform `ssh-keyscan` (%s)", err)
		return err
	}

	if _, err = fmt.Fprintf(f, "%s\n", keyscanOutput); err != nil {
		warningf("Could not write to \"%s\" (%s)", kh.Path, err)
		return err
	}

	commentf("Added \"%s\" to the list of known hosts at \"%s\"", host, kh.Path)
	return nil
}

func findSSHToolsDir(sh *shell.Shell) (string, error) {
	// On Windows, ssh-keygen isn't on the $PATH by default, but we know we can find it
	// relative to where git for windows is installed, so try that
	if runtime.GOOS == "windows" {
		p, err := sh.Subprocess("git", "--exec-path")
		if err != nil {
			return "", err
		}
		gitExecPathOutput, _ := p.RunAndOutput()
		if len(gitExecPathOutput) > 0 {
			sshToolRelativePaths := [][]string{}
			sshToolRelativePaths = append(sshToolRelativePaths, []string{"..", "..", "..", "usr", "bin"})
			sshToolRelativePaths = append(sshToolRelativePaths, []string{"..", "..", "bin"})

			for _, segments := range sshToolRelativePaths {
				segments = append([]string{gitExecPathOutput}, segments...)
				dir := filepath.Join(segments...)
				if _, err := os.Stat(filepath.Join(dir, "ssh-keygen.exe")); err == nil {
					return dir, nil
				}
			}
		}
	}

	keygen, err := exec.LookPath("ssh-keygen")
	if err != nil {
		return "", fmt.Errorf("Failed to find path for ssh-keygen: %v", err)
	}

	return filepath.Dir(keygen), nil
}
