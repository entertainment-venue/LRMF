package LRMF

import "context"

type testTaskProvider struct{}

func (config *testTaskProvider) Tasks(ctx context.Context) ([]Task, error) {
	var tasks []Task
	task1 := &KvTask{K: "key1", V: "value1"}
	task2 := &KvTask{K: "key2", V: "value2"}
	task3 := &KvTask{K: "key3", V: "value3"}
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
