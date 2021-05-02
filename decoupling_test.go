package LRMF

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func Test_fileWatcher(t *testing.T) {
	fw := fileWatcher{
		fileName: "/tmp/foo",
	}

	err := fw.update([]byte(`foo`))
	skipErr(t, err)

	current, err := fw.get()
	skipTrue(t, current != "foo")
}

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

func Test_smoothWorker(t *testing.T) {
	fileName, err := filepath.Abs(defaultFileName)
	skipErr(t, err)

	worker := smoothWorker{
		fw:        &fileWatcher{fileName: fileName},
		eventChan: make(chan *SmoothEvent, 32),
	}

	go func(eventChan <-chan *SmoothEvent) {
		for event := range eventChan {
			fmt.Println(event.Task.Key(context.TODO()), event.Task.Value(context.TODO()), event.Typ)
		}
	}(worker.eventChan)

	var revoke []Task
	revoke = append(revoke, &KvTask{K: "foo", V: "bar"})
	err = worker.Revoke(context.TODO(), revoke)
	skipErr(t, err)

	var assign []Task
	assign = append(assign, &KvTask{K: "foo2", V: "bar2"})
	err = worker.Assign(context.TODO(), assign)
	skipErr(t, err)

	fmt.Println(worker.fw.get())

	done := make(chan bool)
	<-done
}

func Test_Smooth(t *testing.T) {
	eventChan, err := Smooth(
		context.TODO(),
		[]*KvTask{
			{
				K: "foo",
				V: "bar",
			},
		},
		WithEtcdEndpoints([]string{"127.0.0.1:2379"}),
		WithProtocol("foo"),
		WithBiz("bar"),
	)
	skipErr(t, err)

	go func(eventChan <-chan *SmoothEvent) {
		for event := range eventChan {
			fmt.Println(event.Task.Key(context.TODO()), event.Task.Value(context.TODO()), event.Typ)
		}
	}(eventChan)

	done := make(chan bool)
	<-done
}
