package lrmf

import (
	"context"

	"github.com/pkg/errors"
)

type coordinatorOptions struct {
	protocol      string
	biz           string
	tenancy       string
	instanceId    string
	etcdEndpoints []string
	taskHub       TaskHub
	taskProvider  TaskProvider
	assignor      Assignor
}

// coordinator没办法给默认设定，必须精确
var defaultCoordinatorOptions = coordinatorOptions{
	tenancy: "default",
}

type CoordinatorOptionsFunc func(options *coordinatorOptions)

func WithProtocol(v string) CoordinatorOptionsFunc {
	return func(options *coordinatorOptions) {
		options.protocol = v
	}
}

func WithBiz(v string) CoordinatorOptionsFunc {
	return func(options *coordinatorOptions) {
		options.biz = v
	}
}

func WithTenancy(v string) CoordinatorOptionsFunc {
	return func(options *coordinatorOptions) {
		options.tenancy = v
	}
}

func WithInstanceId(v string) CoordinatorOptionsFunc {
	return func(options *coordinatorOptions) {
		options.instanceId = v
	}
}

func WithTaskHub(v TaskHub) CoordinatorOptionsFunc {
	return func(options *coordinatorOptions) {
		options.taskHub = v
	}
}

func WithAssignor(v Assignor) CoordinatorOptionsFunc {
	return func(options *coordinatorOptions) {
		options.assignor = v
	}
}

func WithTaskProvider(v TaskProvider) CoordinatorOptionsFunc {
	return func(options *coordinatorOptions) {
		options.taskProvider = v
	}
}

func WithEtcdEndpoints(v []string) CoordinatorOptionsFunc {
	return func(options *coordinatorOptions) {
		options.etcdEndpoints = v
	}
}

func StartCoordinator(ctx context.Context, optFunc ...CoordinatorOptionsFunc) (*Coordinator, error) {
	opts := defaultCoordinatorOptions
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

	if opts.tenancy == "" {
		return nil, errors.New("Empty tenancy")
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

	coordinator := Coordinator{
		protocol:     opts.protocol,
		instanceId:   opts.instanceId,
		biz:          opts.biz,
		tenancy:      opts.tenancy,
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
