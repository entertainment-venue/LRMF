package lrmf

import (
	"context"
	"encoding/json"
	"fmt"
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

// unit test
type taskTest struct {
	K string `json:"k"`
	V string `json:"v"`
}

func (t *taskTest) Key(ctx context.Context) string {
	return t.K
}

func (t *taskTest) Value(ctx context.Context) string {
	return t.V
}

type testAssignmentParser struct{}

func (p *testAssignmentParser) Unmarshal(ctx context.Context, assignment string) ([]Task, error) {
	var tasks []*taskTest
	if err := json.Unmarshal([]byte(assignment), &tasks); err != nil {
		return nil, errors.Wrapf(err, "FAILED to unmarshal assignment %s", assignment)
	}

	var r []Task
	for _, task := range tasks {
		r = append(r, task)
	}
	return r, nil
}

// unit test
type testTaskProvider struct{}

func (config *testTaskProvider) Tasks(ctx context.Context) ([]Task, error) {
	var tasks []Task
	task1 := &taskTest{K: "key1", V: "value1"}
	task2 := &taskTest{K: "key2", V: "value2"}
	task3 := &taskTest{K: "key3", V: "value3"}
	tasks = append(tasks, task1)
	tasks = append(tasks, task2)
	tasks = append(tasks, task3)
	return tasks, nil
}

func (config *testTaskProvider) Tenancy() string {
	return "default"
}

type testWorker struct {
	// 区分不同的instance
	InstanceId string
}

func (w *testWorker) Revoke(ctx context.Context, tasks []Task) error {
	for _, task := range tasks {
		Logger.Printf("instance %s revoke task %s", w.InstanceId, task.Key(ctx))
	}
	return nil
}

func (w *testWorker) Assign(ctx context.Context, tasks []Task) error {
	for _, task := range tasks {
		Logger.Printf("instance %s assign task %s", w.InstanceId, task.Key(ctx))
	}
	return nil
}
