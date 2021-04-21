## LRMF(Limited Resource Management Framework)

支持以sdk的方式集成到go程序，利用etcd服务实现在不同go进程之间分配任务，类似存储系统中的placement driver角色，这里做的是任务和进程的映射。  
Limited：目前仅支持在单点上做任务分配的计算，计算能力不能横向扩容。映射算法支持定制化，算法能够支持较大量的任务和进程资源的映射，但是映射的结果需要通过etcd在集群范围内的共享实现协调机制，计算和etcd本身都会随着任务量增加成为瓶颈。  
目前的主要应用场景： k级别的任务，由当前go服务组成的worker集群负责处理。 例如：

* kafka mm2中的topic/partition/consumergroup维度的任务拆分和分配。
* 利用redis做延迟队列，对zset做存储/消费速率上的容量扩容，可以使用LRMF做分片与消费进程之间的任务分配。
* 将若干任务在某个维度聚合成group，group与进程之间的分配关系也可以使用LRMF。

## Table of Contents

- [Getting Started](#getting-started)
    - [Installing](#installing)
- [Concept explanation](#concept-explanation)
    - [Task](#task)
    - [TaskProvider](#taskprovider)
    - [Assignor](#assignor)
    - [Worker](#worker)
- [Example](#example)

## Getting Started

### Installing

安装Go，然后运行：
`go get github.com/entertainment-venue/LRMF`

## Concept explanation

### Task

任务数据结构，接口实现以下接口：

```
type Task interface {
	// 按照Key做任务分布
	Key(ctx context.Context) string

	// Value代表实际任务内容
	Value(ctx context.Context) string
}
```

统一使用LRMF内部定义的Task结构体`KvTask`，结构如下：

```
type KvTask struct {
	K string `json:"k"`
	V string `json:"v"`
}
```

### TaskProvider

任务的拆解由使用LRMF的go程序提供，实现以下接口：

```
type TaskProvider interface {
	Tasks(ctx context.Context) ([]Task, error)
}
```

### Assignor

用户可以实现自己的任务/资源映射算法，目前内置的有：

* ConsistentHashingAssignor
* StringOrderEvenlyAssignor

具体实现可以阅读assignor.go源码，接口如下：

```
type Assignor interface {
	PerformAssignment(ctx context.Context, tasks []Task, instanceIds []string) (map[string][]Task, error)
}
```

### Worker

用户需要实现Worker接口，接收(Assign)或者删除(Revoke)任务

```
type Worker interface {
    Revoke(ctx context.Context, tasks []Task) error
    Assign(ctx context.Context, tasks []Task) error
}
```

## Example

```
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

func main() {
	taskProvider := &testTaskProvider{}
	assignor := &StringOrderEvenlyAssignor{}

	worker := &testWorker{InstanceId: instanceId}
	taskHub := NewTaskHub(context.TODO(), worker)

	instanceId := fmt.Sprintf("testInstance_%d", i)

	coordinator, err := StartCoordinator(
		context.TODO(),
		WithEtcdEndpoints([]string{"127.0.0.1:2379"}),
		WithProtocol("foo"),
		WithBiz("bar"),
		WithTenancy("tenancy"),
		WithInstanceId(instanceId),
		WithTaskHub(taskHub),
		WithTaskProvider(taskProvider),
		WithAssignor(assignor))
	if err != nil {
	    panic(err)
	}

	ch := make(chan struct{})
	<-ch
}
```