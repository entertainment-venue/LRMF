package LRMF

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
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
