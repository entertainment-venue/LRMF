package LRMF

import (
	"context"
	"sort"

	"github.com/golang/groupcache/consistenthash"
)

type Assignor interface {
	PerformAssignment(ctx context.Context, tasks []Task, instanceIds []string) (map[string][]Task, error)
}

func NewAssignor() Assignor {
	// TODO 一致性hash满足长期需求
	return &ConsistentHashingAssignor{}
}

type ConsistentHashingAssignor struct{}

// https://github.com/topics/consistent-hashing
// https://github.com/golang/groupcache/blob/master/consistenthash/consistenthash_test.go
// https://ai.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
// task和instance是多对一关系，task可以看作consistent hashing中的kv，instance是存储节点
func (a *ConsistentHashingAssignor) PerformAssignment(ctx context.Context, tasks []Task, instanceIds []string) (map[string][]Task, error) {
	if len(tasks) == 0 || len(instanceIds) == 0 {
		Logger.Printf("FAILED to perform assignment because param err, tasks %+v instanceIds %+v", tasks, instanceIds)
		return nil, ErrParam
	}
	m := consistenthash.New(len(instanceIds), nil)
	m.Add(instanceIds...)
	r := make(map[string][]Task, len(instanceIds))
	for _, task := range tasks {
		instanceId := m.Get(task.Key(ctx))
		r[instanceId] = append(r[instanceId], task)
		Logger.Printf("Task %s assigned to %s", task.Key(ctx), instanceId)
	}
	return r, nil
}

// 特定mq场景
type StringOrderEvenlyAssignor struct{}

func (a *StringOrderEvenlyAssignor) PerformAssignment(ctx context.Context, tasks []Task, instanceIds []string) (map[string][]Task, error) {
	result := make(map[string][]Task)

	plen := len(tasks)
	clen := len(instanceIds)
	if plen == 0 || clen == 0 {
		return result, nil
	}

	tl := taskList(tasks)
	sort.Sort(tl)
	sort.Strings(instanceIds)

	// debug
	instanceIdAndTks := make(map[string][]string)

	n := plen / clen
	m := plen % clen
	p := 0
	for i, instanceId := range instanceIds {
		first := p
		last := first + n
		if m > 0 && i < m {
			last++
		}
		if last > plen {
			last = plen
		}

		for _, t := range tl[first:last] {
			result[instanceId] = append(result[instanceId], t)

			// debug
			instanceIdAndTks[instanceId] = append(instanceIdAndTks[instanceId], t.Key(context.TODO()))
		}
		p = last
	}

	// debug
	var tks []string
	for _, t := range tasks {
		tks = append(tks, t.Key(context.TODO()))
	}

	Logger.Printf("Divided: \n tasks => (%+v) \n instanceId => (%+v) \n result => (%+v) \n", tks, instanceIds, result)

	return result, nil
}
