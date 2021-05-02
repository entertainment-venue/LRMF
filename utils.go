package LRMF

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
)

// 先干活，在进入周期运行
func doAndLoop(ctx context.Context, duration time.Duration, fn func(ctx context.Context) error, caller string) {

	var err error

	err = fn(ctx)
	if err != nil {
		Logger.Printf("%+v", err)
	}

	for {
		select {
		case <-ctx.Done():
			Logger.Printf("%s exit", caller)
			return

		case <-time.After(duration):
			err = fn(ctx)
			if err != nil {
				Logger.Printf("%+v", err)
			}
		}
	}
}

type stateValue string

func (v stateValue) StateAndLeaseID() (string, int64) {
	arr := strings.Split(string(v), "_")
	state := arr[0]
	leaseID, _ := strconv.ParseInt(arr[1], 10, 64)
	return state, leaseID
}

func (v stateValue) String() string {
	return string(v)
}

func formatStateValue(state string, leaseID clientv3.LeaseID) string {
	return fmt.Sprintf("%s_%d", state, leaseID)
}

func withRecover(ctx context.Context, fn func(ctx context.Context)) {
	defer func() {
		if err := recover(); err != nil {
			Logger.Printf("panic happened: %v", err)

			// 打印堆栈，方便问题追查
			var buf [4096]byte
			n := runtime.Stack(buf[:], false)
			Logger.Printf("panic: %s", string(buf[:n]))
		}
	}()

	fn(ctx)
}

func mkdirIfNotExist(path string) error {
	dir := filepath.Dir(path)

	var err error

	// 存在且err!=nil的情况不考虑
	_, err = os.Stat(dir)

	if os.IsNotExist(err) {
		err = os.MkdirAll(dir, os.ModePerm)
	}

	return errors.Wrap(err, "")
}

func getLocalIp() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func ParseKvTask(tasks []Task) []*KvTask {
	if tasks == nil {
		return nil
	}

	var r []*KvTask
	for _, task := range tasks {
		r = append(r, task.(*KvTask))
	}
	return r
}
