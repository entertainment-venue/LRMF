package LRMF

import (
	"context"

	"github.com/pkg/errors"
)

// 为了提升易用性、稳定性，基于LRMF的通用功能，进一步将使用方式固化
// 1. 只需要按照固定格式提供任务列表，各instance不需要关注自己是否是leader，只要保证所有业务进程对于全局任务包含的内容达成一致即可
// 2. 利用磁盘将任务划分结果持久化，然后利用感知磁盘文件变化的goroutine通知给业务逻辑，解耦LRMF和用户逻辑。
// 第2点有解决两个问题：
// LRMF任务OnAssign依赖本地地盘（LRMF不依赖于业务代码的实现，任务分配机制收到的干扰少，成功率更高，问题排查容易）；
// 单独的goroutine负责将任务分配结果给到业务逻辑，设计灵活度更大，灾难恢复手段更简单，重启/恢复不以来LRMF，直接开始干活（考虑static membership是否还需要）。

type EventType int

const (
	EventRevoke EventType = iota
	EventAssign
)

type SmoothEvent struct {
	Typ  EventType
	Task Task
}

type smoothCallbackFunc func(*SmoothEvent)

func Smooth(ctx context.Context, tasks []*KvTask, callbackFunc smoothCallbackFunc, optFunc ...CoordinatorOptionsFunc) error {
	opts := defaultCoordinatorOptions
	for _, of := range optFunc {
		of(&opts)
	}

	if opts.protocol == "" {
		return errors.New("Empty protocol")
	}

	if opts.biz == "" {
		return errors.New("Empty biz")
	}

	if opts.tenancy == "" {
		return errors.New("Empty tenancy")
	}

	if len(opts.etcdEndpoints) < 1 {
		return errors.New("Empty etcd endpoints")
	}

	if opts.instanceId == "" {
		opts.instanceId = getLocalIp()
	}

	provider := smoothTaskProvider{tasks: tasks}

	worker := smoothWorker{
		callbackFunc: callbackFunc,
	}

	taskHub := NewTaskHub(ctx, &worker)

	instanceId := getLocalIp()
	if instanceId == "" {
		return errors.New("ip fetch failed")
	}

	if _, err := StartCoordinator(
		ctx,
		WithEtcdEndpoints(opts.etcdEndpoints),
		WithProtocol(opts.protocol),
		WithBiz(opts.biz),
		WithTenancy(opts.tenancy),
		WithInstanceId(instanceId),
		WithTaskHub(taskHub),
		WithTaskProvider(&provider),
		WithAssignor(&StringOrderEvenlyAssignor{})); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

type smoothTaskProvider struct {
	tasks []*KvTask
}

func (provider *smoothTaskProvider) Tasks(_ context.Context) ([]Task, error) {
	var tasks []Task
	for _, task := range provider.tasks {
		tasks = append(tasks, task)
	}
	return tasks, nil
}

type smoothWorker struct {
	callbackFunc smoothCallbackFunc
}

func (worker *smoothWorker) Revoke(_ context.Context, tasks []Task) error {
	for _, rt := range tasks {
		worker.callbackFunc(&SmoothEvent{Typ: EventRevoke, Task: rt})
	}
	return nil
}

func (worker *smoothWorker) Assign(_ context.Context, tasks []Task) error {
	for _, at := range tasks {
		worker.callbackFunc(&SmoothEvent{Typ: EventRevoke, Task: at})
	}
	return nil
}
