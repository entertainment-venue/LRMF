package lrmf

import (
	"context"
	"testing"
)

func Test_stringOrderEvenlyAssignor(t *testing.T) {

	var (
		tasks       []Task
		instanceIds []string
		result      map[string][]Task
		err         error
	)

	assignor := StringOrderEvenlyAssignor{}

	// 参数空
	result, err = assignor.PerformAssignment(context.TODO(), tasks, instanceIds)
	skipErr(t, err)
	if len(result) > 0 {
		t.Errorf("Should not have result, %+v", result)
		t.SkipNow()
	}

	// 任务多，instance少
	tasks = []Task{
		&taskTest{K: "b"},
		&taskTest{K: "c"},
		&taskTest{K: "a"},
	}
	instanceIds = []string{
		"foo",
		"bar",
	}
	result, err = assignor.PerformAssignment(context.TODO(), tasks, instanceIds)
	skipErr(t, err)
	if len(result["foo"]) != 1 || len(result["bar"]) != 2 {
		t.Errorf("result count err: %+v", result)
		t.SkipNow()
	}

	// 任务少，instance多
	tasks = []Task{
		&taskTest{K: "foo"},
		&taskTest{K: "bar"},
	}
	instanceIds = []string{
		"c",
		"b",
		"a",
	}
	result, err = assignor.PerformAssignment(context.TODO(), tasks, instanceIds)
	skipErr(t, err)
	if len(result["a"]) != 1 || len(result["b"]) != 1 {
		t.Errorf("result count err: %+v", result)
		t.SkipNow()
	}
}
