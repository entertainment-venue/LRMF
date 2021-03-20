package lrmf

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type none struct{}

// 具体业务场景下，需要业务实现，是Worker要求业务实现的
type WorkerHub interface {
	Revoke(ctx context.Context, tasks []Task) error
	Assign(ctx context.Context, tasks []Task) error
}

type Worker interface {
	Start(ctx context.Context, task Task) error
}

type WorkerFactory interface {
	New(ctx context.Context, task Task) (Worker, error)
}

// 基本工作模式是每个task对goroutine
// 标记goroutine，能够支持revoke和assign，回收和新增goroutine创建新的任务
type workerHub struct {
	gp *goroutinePool

	workerFactory WorkerFactory
}

// 具体业务场景下调用，给到coordinator
func NewWorkHub(wf WorkerFactory) WorkerHub {
	return &workerHub{
		gp:            &goroutinePool{kAndStopper: make(map[string]*goroutineStopper)},
		workerFactory: wf,
	}
}

func (p *workerHub) Revoke(ctx context.Context, tasks []Task) error {
	for _, task := range tasks {
		p.gp.Remove(ctx, task)
	}
	return nil
}

func (p *workerHub) Assign(ctx context.Context, tasks []Task) error {
	for _, task := range tasks {
		w, err := p.workerFactory.New(ctx, task)
		if err != nil {
			return errors.Wrap(err, "")
		}
		p.gp.Add(ctx, task, w.Start)
	}
	return nil
}

// 需要goroutine pool，支持start 和 stop指定goroutine的能力，
// 但是这个goroutine的方法是用户编写，框架层能够等待用户执行完成，
// 然后识别是否继续或者销毁goroutine与dispatcher不同，不再是生产
// 消费的模式，所有变化都有api触发
type DoFunc func(ctx context.Context, task Task) error

type goroutinePool struct {
	mu          sync.Mutex
	kAndStopper map[string]*goroutineStopper
}

type goroutineStopper struct {
	// 触发goroutine退出
	stopFunc context.CancelFunc
	// 等待业务方法退出，业务方法需要支持ctx.Done控制
	notifyCh chan none
}

// 不接受失败的场景
func (p *goroutinePool) Add(ctx context.Context, task Task, doFunc DoFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()

	k := task.Key(ctx)
	_, ok := p.kAndStopper[k]
	if ok {
		Logger.Printf("FAILED to add k=%s, already exist", k)
		return
	}

	// 这里需要使用全新的 ctx 来管理，避免上层 ctx 的关闭导致下面干活的 worker 关闭
	cancelCtx, cancel := context.WithCancel(context.Background())
	stopper := goroutineStopper{
		stopFunc: cancel,
		notifyCh: make(chan none),
	}
	p.kAndStopper[k] = &stopper

	go func(pCtx context.Context, blockFunc DoFunc, pTask Task, pStopper *goroutineStopper) {

	doTask:
		// 该方法返回值为 nil 时，代表任务执行成功，work 直接退出即可，err 时无限重试
		if err := blockFunc(pCtx, task); err != nil {
			Logger.Printf("FAILED to call DoFunc err %s", err)
			time.Sleep(defaultOpWaitTimeout)
			goto doTask
		}

		Logger.Printf("task goroutine exit, k = %s v = %s", pTask.Key(ctx), pTask.Value(ctx))
		pStopper.notifyCh <- none{}

	}(cancelCtx, doFunc, task, &stopper)
}

func (p *goroutinePool) Remove(ctx context.Context, task Task) {
	p.mu.Lock()
	defer p.mu.Unlock()

	k := task.Key(ctx)
	stopper, ok := p.kAndStopper[k]
	if !ok {
		Logger.Printf("FAILED to remove k=%s, not exist", k)
		return
	}

	stopper.stopFunc()
	<-stopper.notifyCh
	delete(p.kAndStopper, k)

	Logger.Printf("Successfully remove k %s", k)
}
