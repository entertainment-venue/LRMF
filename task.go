package lrmf

import (
	"context"

	"github.com/pkg/errors"
)

type Task interface {
	// 按照Key做任务分布
	Key(ctx context.Context) string

	// Value代表实际任务内容
	Value(ctx context.Context) string
}

type taskList []Task

func (tl taskList) Len() int {
	return len(tl)
}

func (tl taskList) Less(i, j int) bool {
	return tl[i].Key(context.TODO()) < tl[j].Key(context.TODO())
}

func (tl taskList) Swap(i, j int) {
	tl[i], tl[j] = tl[j], tl[i]
}

type AssignmentParser interface {
	Unmarshal(ctx context.Context, assignment string) ([]Task, error)
}

// 解决抽象层面的问题，对接coordinator，相当于抽象类
type TaskHub interface {
	// 提供剔除目标
	OnRevoked(ctx context.Context, revoke string) ([]Task, error)

	// 提供分配结果
	OnAssigned(ctx context.Context, assignment string) ([]Task, error)

	Assignment(ctx context.Context) string

	UnmarshalAssignment(ctx context.Context, assignment string) ([]Task, error)
}

type TaskProvider interface {
	Tasks(ctx context.Context) ([]Task, error)

	// 支持多租户
	Tenancy() string
}

type taskHub struct {
	// 需要存储原始任务数据
	rawAssignment string

	// 下发revoke/assign
	workerHub WorkerHub

	// 转化assignment到tasks
	assignmentParser AssignmentParser
}

// 包外访问
func NewTaskHub(ctx context.Context, workerHub WorkerHub, parser AssignmentParser) TaskHub {
	// assignment的附值等rb
	return &taskHub{
		workerHub:        workerHub,
		assignmentParser: parser,
	}
}

func (w *taskHub) OnRevoked(ctx context.Context, assignment string) ([]Task, error) {
	newTasks, err := w.UnmarshalAssignment(ctx, assignment)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	assignedTasks, err := w.UnmarshalAssignment(ctx, w.rawAssignment)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	var revoke []Task
	for _, assignedTask := range assignedTasks {
		var exist bool
		for _, task := range newTasks {
			if task.Key(ctx) == assignedTask.Key(ctx) {
				exist = true
				break
			}
		}
		if !exist {
			revoke = append(revoke, assignedTask)
		}
	}
	if len(revoke) > 0 {
		if err := w.workerHub.Revoke(ctx, revoke); err != nil {
			return nil, errors.Wrapf(err, "FAILED to revoke %+v", revoke)
		}
	}

	return revoke, nil
}

// 分配任务后需要经过revoke和assign两个阶段
func (w *taskHub) OnAssigned(ctx context.Context, assignment string) ([]Task, error) {
	newTasks, err := w.UnmarshalAssignment(ctx, assignment)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	assignedTasks, err := w.UnmarshalAssignment(ctx, w.rawAssignment)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	var assign []Task
	for _, newTask := range newTasks {
		var exist bool
		for _, assignedTask := range assignedTasks {
			if assignedTask.Key(ctx) == newTask.Key(ctx) {
				exist = true
				break
			}
		}
		if !exist {
			assign = append(assign, newTask)
		}
	}
	if len(assign) > 0 {
		if err := w.workerHub.Assign(ctx, assign); err != nil {
			return nil, errors.Wrapf(err, "FAILED to assign %+v", assign)
		}
	}

	// assign完成，才能更改当前instance负责的任务
	w.rawAssignment = assignment

	return assign, nil
}

// hb时使用；提供给leader在rb时做是否立即rb决策；周期检查当前cluster的balance状态
func (w *taskHub) Assignment(ctx context.Context) string {
	return w.rawAssignment
}

func (w *taskHub) UnmarshalAssignment(ctx context.Context, assignment string) ([]Task, error) {
	var (
		r   []Task
		err error
	)

	// 当前没有任务的场景
	if assignment == "" {
		return r, nil
	}

	r, err = w.assignmentParser.Unmarshal(ctx, assignment)
	if err != nil {
		return nil, errors.Wrapf(err, "FAILED to unmarshal %s", assignment)
	}
	return r, nil
}
