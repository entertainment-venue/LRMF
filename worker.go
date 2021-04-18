package LRMF

import (
	"context"

	"github.com/pkg/errors"
)

type none struct{}

// 具体业务场景下，需要业务实现，是Worker要求业务实现的
type Worker interface {
	Revoke(ctx context.Context, tasks []Task) error
	Assign(ctx context.Context, tasks []Task) error
}

type WorkerStarter interface {
	Start(ctx context.Context, task Task) error
}

type WorkerFactory interface {
	New(ctx context.Context, task Task) (WorkerStarter, error)
}

// 基本工作模式是每个task对goroutine
// 标记goroutine，能够支持revoke和assign，回收和新增goroutine创建新的任务
type worker struct {
	gp *goroutinePool

	workerFactory WorkerFactory
}

// 具体业务场景下调用，给到coordinator
func NewWorker(wf WorkerFactory) Worker {
	return &worker{
		gp:            &goroutinePool{kAndStopper: make(map[string]*goroutineStopper)},
		workerFactory: wf,
	}
}

func (p *worker) Revoke(ctx context.Context, tasks []Task) error {
	for _, task := range tasks {
		p.gp.Remove(ctx, task)
	}
	return nil
}

func (p *worker) Assign(ctx context.Context, tasks []Task) error {
	for _, task := range tasks {
		w, err := p.workerFactory.New(ctx, task)
		if err != nil {
			return errors.Wrap(err, "")
		}
		p.gp.Add(ctx, task, w.Start)
	}
	return nil
}
