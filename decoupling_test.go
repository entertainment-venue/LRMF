package LRMF

import (
	"context"
	"fmt"
	"testing"
)

func Test_smoothTaskProvider(t *testing.T) {
	provider := smoothTaskProvider{
		tasks: []*KvTask{
			{
				K: "foo",
				V: "bar",
			},
		},
	}

	tasks, err := provider.Tasks(context.TODO())
	skipErr(t, err)

	for _, task := range tasks {
		fmt.Println(task.Key(context.TODO()), task.Value(context.TODO()))
	}
}

func Sample(event *SmoothEvent) {
	fmt.Println(event.Task.Key(context.TODO()), event.Task.Value(context.TODO()), event.Typ)

}

func Test_Smooth(t *testing.T) {
	err := Smooth(
		context.TODO(),
		[]*KvTask{
			{
				K: "foo",
				V: "bar",
			},
		},
		Sample,
		WithEtcdEndpoints([]string{"127.0.0.1:2379"}),
		WithProtocol("foo"),
		WithBiz("bar"),
	)
	skipErr(t, err)

	done := make(chan bool)
	<-done
}
