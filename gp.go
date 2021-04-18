package LRMF

import (
	"context"
	"sync"
	"time"
)

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
