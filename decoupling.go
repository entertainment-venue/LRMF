package LRMF

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
	"path/filepath"
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

func Smooth(ctx context.Context, tasks []*KvTask, optFunc ...CoordinatorOptionsFunc) (<-chan *SmoothEvent, error) {
	opts := defaultCoordinatorOptions
	for _, of := range optFunc {
		of(&opts)
	}

	if opts.protocol == "" {
		return nil, errors.New("Empty protocol")
	}

	if opts.biz == "" {
		return nil, errors.New("Empty biz")
	}

	if opts.tenancy == "" {
		return nil, errors.New("Empty tenancy")
	}

	if len(opts.etcdEndpoints) < 1 {
		return nil, errors.New("Empty etcd endpoints")
	}

	if opts.instanceId == "" {
		opts.instanceId = getLocalIp()
	}

	provider := smoothTaskProvider{tasks: tasks}

	// 128取舍没有理由，业务尽快处理掉得到的任务
	fileName, err := filepath.Abs(defaultFileName)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	worker := smoothWorker{
		fw:        &fileWatcher{fileName: fileName},
		eventChan: make(chan *SmoothEvent, 128),
	}

	taskHub := NewTaskHub(ctx, &worker)

	instanceId := getLocalIp()
	if instanceId == "" {
		return nil, errors.New("ip fetch failed")
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
		return nil, errors.Wrap(err, "")
	}

	return worker.eventChan, nil
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
	// assign/revoke网络交互的过程与
	fw *fileWatcher

	eventChan chan *SmoothEvent
}

func (worker *smoothWorker) Revoke(_ context.Context, tasks []Task) error {
	// revoke会清理掉之前的assign
	fc := fileContent{
		Revoke: ParseKvTask(tasks),
	}

	if err := worker.saveToFile(&fc); err != nil {
		return errors.Wrap(err, "")
	}

	for _, rt := range tasks {
		worker.eventChan <- &SmoothEvent{Typ: EventRevoke, Task: rt}
	}
	return nil
}

func (worker *smoothWorker) Assign(_ context.Context, tasks []Task) error {
	fc := fileContent{}
	current, err := worker.fw.get()
	if err != nil {
		return errors.Wrap(err, "")
	}
	if err := json.Unmarshal([]byte(current), &fc); err != nil {
		return errors.Wrap(err, "")
	}

	fc.Revoke = nil
	fc.Assign = ParseKvTask(tasks)

	if err := worker.saveToFile(&fc); err != nil {
		return errors.Wrap(err, "")
	}
	for _, at := range tasks {
		worker.eventChan <- &SmoothEvent{Typ: EventAssign, Task: at}
	}
	return nil
}

func (worker *smoothWorker) saveToFile(fc *fileContent) error {
	b := fc.json()
	// 只更新本地文件，保证成功率，业务层保证任务不被多个进程同时抢占
	if err := worker.fw.update(b); err != nil {
		return errors.Wrap(err, "")
	}
	Logger.Printf("save tasks: %s", string(b))
	return nil
}

const defaultFileName = "var/LRMF_tasks.json"

// 封装文件操作，这块也会成为每台机器任务分配数量的瓶颈，可以分文件存储提升落盘速度和提取速度，不过在io这块需要验证。
// 或者干脆选择boltdb这类单机的kv存储，第三方的通常会做好评测。
// 后续会考虑把接口定义出来，方便更换实现。
type fileWatcher struct {
	// defaultFileName绝对路径
	fileName string
}

type fileContent struct {
	Revoke []*KvTask `json:"revoke"`
	Assign []*KvTask `json:"assign"`
}

func (fc *fileContent) json() []byte {
	b, _ := json.Marshal(fc)
	return b
}

func (l *fileWatcher) update(data []byte) error {
	// https://gobyexample.com/writing-files
	err := ioutil.WriteFile(l.fileName, data, 0664)
	return errors.Wrap(err, "")
}

func (l *fileWatcher) get() (string, error) {
	b, err := ioutil.ReadFile(l.fileName)
	if err != nil {
		return "", errors.Wrap(err, "")
	}
	return string(b), nil
}
