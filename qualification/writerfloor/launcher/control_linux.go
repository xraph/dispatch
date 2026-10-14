//go:build linux

package launcher

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type control struct {
	listener net.Listener
	result   chan error
}

func startControl(root, id string, cancel context.CancelFunc) (*control, error) {
	listener, e := (&net.ListenConfig{}).Listen(context.Background(), "unix", filepath.Join(root, id+"-control.sock"))
	if e != nil {
		return nil, e
	}
	if e = os.Chmod(filepath.Join(root, id+"-control.sock"), 0o600); e != nil {
		_ = listener.Close()
		return nil, e
	}
	c := &control{listener, make(chan error, 1)}
	go func() {
		connection, e := listener.Accept()
		if e != nil {
			return
		}
		defer connection.Close()
		if deadlineErr := connection.SetDeadline(time.Now().Add(20 * time.Second)); deadlineErr != nil {
			return
		}
		raw := make([]byte, 5)
		n, e := io.ReadFull(connection, raw)
		if e != nil || n != 5 || string(raw) != "stop\n" {
			return
		}
		cancel()
		result := <-c.result
		if result == nil {
			if _, writeErr := connection.Write([]byte("stopped\n")); writeErr != nil {
				return
			}
		} else {
			if _, writeErr := connection.Write([]byte("uncertain\n")); writeErr != nil {
				return
			}
		}
	}()
	return c, nil
}
func (c *control) finish(result error) { c.result <- result; _ = c.listener.Close() }
func requestStop(ctx context.Context, root, id string) (bool, error) {
	if !token(id) {
		return true, ErrRefused
	}
	if operationErr := secureDirectory(root); operationErr != nil {
		return true, operationErr
	}
	socket := filepath.Join(root, id+"-control.sock")
	info, e := os.Lstat(socket)
	if errors.Is(e, os.ErrNotExist) {
		return false, nil
	}
	if e != nil {
		return true, e
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != 0 || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		return true, ErrRefused
	}
	connection, e := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", socket)
	if errors.Is(e, syscall.ECONNREFUSED) {
		return false, nil
	}
	if e != nil {
		return true, e
	}
	defer connection.Close()
	deadline := time.Now().Add(20 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if operationErr := connection.SetDeadline(deadline); operationErr != nil {
		return true, operationErr
	}
	if _, e = connection.Write([]byte("stop\n")); e != nil {
		return true, e
	}
	result, e := bufio.NewReaderSize(connection, 32).ReadString('\n')
	if e != nil || result != "stopped\n" {
		return true, ErrUncertain
	}
	return true, nil
}
