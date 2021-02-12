package lrmf

import (
	"context"

	"github.com/pkg/errors"
)

type workerCoordinatorOptions struct {
	protocol      string
	biz           string
	instanceId    string
	etcdEndpoints []string
	taskHub       TaskHub
	taskProvider  TaskProvider
	assignor      Assignor
}

// coordinator没办法给默认设定，必须精确
var defaultWorkerCoordinatorOptions = workerCoordinatorOptions{}

type workerCoordinatorOptionsFunc func(options *workerCoordinatorOptions)

func WithProtocol(v string) workerCoordinatorOptionsFunc {
	return func(options *workerCoordinatorOptions) {
		options.protocol = v
	}
}

func WithBiz(v string) workerCoordinatorOptionsFunc {
	return func(options *workerCoordinatorOptions) {
		options.biz = v
	}
}

func WithInstanceId(v string) workerCoordinatorOptionsFunc {
	return func(options *workerCoordinatorOptions) {
		options.instanceId = v
	}
}

func WithTaskHub(v TaskHub) workerCoordinatorOptionsFunc {
	return func(options *workerCoordinatorOptions) {
		options.taskHub = v
	}
}

func WithAssignor(v Assignor) workerCoordinatorOptionsFunc {
	return func(options *workerCoordinatorOptions) {
		options.assignor = v
	}
}

func WithTaskProvider(v TaskProvider) workerCoordinatorOptionsFunc {
	return func(options *workerCoordinatorOptions) {
		options.taskProvider = v
	}
}

func WithEtcdEndpoints(v []string) workerCoordinatorOptionsFunc {
	return func(options *workerCoordinatorOptions) {
		options.etcdEndpoints = v
	}
}

func StartWorkerCoordinator(ctx context.Context, optFunc ...workerCoordinatorOptionsFunc) (*WorkerCoordinator, error) {
	opts := defaultWorkerCoordinatorOptions
	for _, of := range optFunc {
		of(&opts)
	}

	if opts.protocol == "" {
		return nil, errors.New("Empty protocol")
	}

	// instanceId是核心点
	if opts.instanceId == "" {
		return nil, errors.New("Empty instanceId")
	}

	if opts.biz == "" {
		return nil, errors.New("Empty biz")
	}

	if opts.taskHub == nil {
		return nil, errors.New("Empty taskHub")
	}

	if opts.taskProvider == nil {
		return nil, errors.New("Empty taskProvider")
	}

	if len(opts.etcdEndpoints) < 1 {
		return nil, errors.New("Empty etcd address")
	}

	// 初始化coordinator，允许当前instance承载不同protocol的任务，coordinator只负责下发任务

	coordinator := WorkerCoordinator{
		protocol:     opts.protocol,
		instanceId:   opts.instanceId,
		biz:          opts.biz,
		taskHub:      opts.taskHub,
		taskProvider: opts.taskProvider,
		assignor:     opts.assignor,
	}

	// etcd初始化
	var err error
	coordinator.etcdWrapper, err = NewEtcdWrapper(ctx, opts.etcdEndpoints, &coordinator)
	if err != nil {
		// 注意这里不能无限等待，直接告知上游err即可
		return nil, errors.Wrap(err, "")
	}

	if err := coordinator.JoinGroup(ctx); err != nil {
		return nil, errors.Wrap(err, "")
	}

	Logger.Printf("Successfully join group %+v", coordinator)

	return &coordinator, nil
}
