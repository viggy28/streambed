//go:build darwin || linux

package supervisor

import "syscall"

// procAttr sets a new process group on the child so the whole tree can be
// signalled with killpg. On macOS and Linux both Setpgid=true works.
func procAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// killProcessGroup sends SIGKILL to every process in the group whose leader
// is pid (negative argument = process group to Kill(2)).
func killProcessGroup(pid int) error {
	return syscall.Kill(-pid, syscall.SIGKILL)
}
