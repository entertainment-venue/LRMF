package LRMF

import (
	"context"

	"github.com/pkg/errors"
)

// 把任务列表和callback方法作为参数，要求业务传递，简化使用场景，常规任务分配，不需要实现interface，业务的callback要做到同步，保证LRMF分配的结果
// 得到妥善处理，异常情况分析：
// 1 应用bug导致callback卡住不再执行，此次g会因为rb超时结束掉，leader会触发新一轮，如果一直卡住，会导致任务分配机制瘫痪
// 2 leader保证全局只有一个rb，保持整体机制简单，不适应大量节点参与

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
