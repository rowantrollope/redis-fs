package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	mountBackendAuto = "auto"
	mountBackendFuse = "fuse"
	mountBackendNFS  = "nfs"
)

type mountStartResult struct {
	PID      int
	Endpoint string
}

type mountBackend interface {
	Name() string
	Start(cfg config) (mountStartResult, error)
	WaitForMount(cfg config, started mountStartResult, timeout time.Duration) error
	IsMounted(mountpoint string) bool
	Unmount(mountpoint string) error
}

func defaultMountBackend() string {
	if runtime.GOOS == "darwin" {
		return mountBackendNFS
	}
	return mountBackendFuse
}

func normalizeMountBackend(v string) (string, error) {
	b := strings.ToLower(strings.TrimSpace(v))
	if b == "" || b == mountBackendAuto {
		return defaultMountBackend(), nil
	}
	switch b {
	case mountBackendFuse, mountBackendNFS:
		return b, nil
	default:
		return "", fmt.Errorf("unsupported mount backend %q (expected auto, fuse, or nfs)", v)
	}
}

func backendForConfig(cfg config) (mountBackend, string, error) {
	name, err := normalizeMountBackend(cfg.MountBackend)
	if err != nil {
		return nil, "", err
	}
	b, err := backendByName(name)
	if err != nil {
		return nil, "", err
	}
	return b, name, nil
}

func backendForState(st state) (mountBackend, string, error) {
	name := st.MountBackend
	if name == "" {
		name = mountBackendFuse
	}
	b, err := backendByName(name)
	if err != nil {
		return nil, "", err
	}
	return b, name, nil
}

func backendByName(name string) (mountBackend, error) {
	switch name {
	case mountBackendFuse:
		return fuseBackend{}, nil
	case mountBackendNFS:
		return nfsBackend{}, nil
	default:
		return nil, fmt.Errorf("unsupported mount backend %q", name)
	}
}

type fuseBackend struct{}

func (f fuseBackend) Name() string { return mountBackendFuse }

func (f fuseBackend) Start(cfg config) (mountStartResult, error) {
	if err := os.MkdirAll(filepathDir(cfg.MountLog), 0o755); err != nil {
		return mountStartResult{}, err
	}
	logFile, err := os.OpenFile(cfg.MountLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return mountStartResult{}, err
	}
	defer logFile.Close()

	args := []string{
		"--redis", cfg.RedisAddr,
		"--db", strconv.Itoa(cfg.RedisDB),
		"--foreground",
		cfg.RedisKey,
		cfg.Mountpoint,
	}
	if cfg.RedisPassword != "" {
		args = append([]string{"--password", cfg.RedisPassword}, args...)
	}
	if cfg.ReadOnly {
		args = append([]string{"--readonly"}, args...)
	}
	if cfg.AllowOther {
		args = append([]string{"--allow-other"}, args...)
	}

	cmd := exec.Command(cfg.MountBin, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if devNull, err := os.Open(os.DevNull); err == nil {
		defer devNull.Close()
		cmd.Stdin = devNull
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		return mountStartResult{}, fmt.Errorf("start mount failed: %w", err)
	}
	pid := cmd.Process.Pid
	_ = cmd.Process.Release()
	return mountStartResult{PID: pid}, nil
}

func (f fuseBackend) WaitForMount(cfg config, _ mountStartResult, timeout time.Duration) error {
	return waitForMountpoint(cfg.Mountpoint, timeout, f.IsMounted)
}

func (f fuseBackend) IsMounted(mountpoint string) bool {
	return mountTableContains(mountpoint)
}

func (f fuseBackend) Unmount(mountpoint string) error {
	for _, c := range [][]string{{"fusermount", "-u", mountpoint}, {"fusermount", "-uz", mountpoint}, {"umount", "-l", mountpoint}, {"umount", mountpoint}} {
		if exec.Command(c[0], c[1:]...).Run() == nil {
			return nil
		}
	}
	return errors.New("all unmount commands failed")
}

type nfsBackend struct{}

func (n nfsBackend) Name() string { return mountBackendNFS }

func nfsExportPath(redisKey string) string {
	trimmed := strings.Trim(redisKey, " /")
	if trimmed == "" {
		return "/fs"
	}
	return "/" + trimmed
}

func (n nfsBackend) Start(cfg config) (mountStartResult, error) {
	if err := os.MkdirAll(filepathDir(cfg.MountLog), 0o755); err != nil {
		return mountStartResult{}, err
	}
	logFile, err := os.OpenFile(cfg.MountLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return mountStartResult{}, err
	}
	defer logFile.Close()

	host := cfg.NFSHost
	if host == "" {
		host = "127.0.0.1"
	}
	port := cfg.NFSPort
	if port <= 0 {
		port = 20490
	}
	export := nfsExportPath(cfg.RedisKey)

	args := []string{
		"--redis", cfg.RedisAddr,
		"--db", strconv.Itoa(cfg.RedisDB),
		"--listen", net.JoinHostPort(host, strconv.Itoa(port)),
		"--export", export,
		"--foreground",
	}
	if cfg.RedisPassword != "" {
		args = append([]string{"--password", cfg.RedisPassword}, args...)
	}
	if cfg.ReadOnly {
		args = append([]string{"--readonly"}, args...)
	}

	cmd := exec.Command(cfg.NFSBin, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if devNull, err := os.Open(os.DevNull); err == nil {
		defer devNull.Close()
		cmd.Stdin = devNull
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		return mountStartResult{}, fmt.Errorf("start nfs gateway failed: %w", err)
	}
	pid := cmd.Process.Pid
	_ = cmd.Process.Release()
	endpoint := fmt.Sprintf("%s:%s", host, export)
	return mountStartResult{PID: pid, Endpoint: endpoint}, nil
}

func (n nfsBackend) WaitForMount(cfg config, started mountStartResult, timeout time.Duration) error {
	addr := cfg.NFSHost
	if addr == "" {
		addr = "127.0.0.1"
	}
	port := cfg.NFSPort
	if port <= 0 {
		port = 20490
	}
	server := net.JoinHostPort(addr, strconv.Itoa(port))

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", server, 250*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(150 * time.Millisecond)
	}

	if !n.IsMounted(cfg.Mountpoint) {
		if err := n.mountLocal(cfg, started.Endpoint); err != nil {
			return err
		}
	}
	return waitForMountpoint(cfg.Mountpoint, timeout, n.IsMounted)
}

func (n nfsBackend) mountLocal(cfg config, endpoint string) error {
	serverPath := endpoint
	if serverPath == "" {
		host := cfg.NFSHost
		if host == "" {
			host = "127.0.0.1"
		}
		serverPath = fmt.Sprintf("%s:%s", host, nfsExportPath(cfg.RedisKey))
	}
	port := cfg.NFSPort
	if port <= 0 {
		port = 20490
	}

	if runtime.GOOS == "darwin" {
		opts := fmt.Sprintf("vers=3,tcp,port=%d,mountport=%d,nolocks", port, port)
		cmd := exec.Command("/sbin/mount_nfs", "-o", opts, serverPath, cfg.Mountpoint)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("mount_nfs failed: %w (%s)", err, strings.TrimSpace(string(out)))
		}
		return nil
	}

	opts := fmt.Sprintf("vers=3,tcp,port=%d,mountport=%d,nolock", port, port)
	cmd := exec.Command("mount", "-t", "nfs", "-o", opts, serverPath, cfg.Mountpoint)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("mount -t nfs failed: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (n nfsBackend) IsMounted(mountpoint string) bool {
	return mountTableContains(mountpoint)
}

func (n nfsBackend) Unmount(mountpoint string) error {
	for _, c := range [][]string{{"umount", mountpoint}, {"umount", "-l", mountpoint}} {
		if exec.Command(c[0], c[1:]...).Run() == nil {
			return nil
		}
	}
	return errors.New("all unmount commands failed")
}

func waitForMountpoint(mountpoint string, timeout time.Duration, mountedFn func(string) bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mountedFn(mountpoint) {
			return nil
		}
		time.Sleep(150 * time.Millisecond)
	}
	return errors.New("timeout waiting for mount")
}

func mountTableContains(mountpoint string) bool {
	out, err := exec.Command("mount").Output()
	if err == nil {
		needle := " on " + mountpoint + " "
		for _, ln := range strings.Split(string(out), "\n") {
			if strings.Contains(ln, needle) {
				return true
			}
		}
	}

	if runtime.GOOS == "linux" {
		b, err := os.ReadFile("/proc/mounts")
		if err == nil {
			return strings.Contains(string(b), " "+mountpoint+" ")
		}
	}
	return false
}

func filepathDir(p string) string {
	if p == "" {
		return "."
	}
	return filepath.Dir(p)
}
