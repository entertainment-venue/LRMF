package lrmf

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
)

func Test_leaderCampaign(t *testing.T) {
	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)
	coordinator.etcdWrapper = wrapper
	coordinator.instanceId = "testInstance"
	coordinator.curG = &G{Id: 1}

	coordinator.leaderCamp(context.TODO())
}

func Test_triggerRb(t *testing.T) {
	clearData(t)

	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)
	coordinator.etcdWrapper = wrapper
	coordinator.instanceId = "testInstance"
	coordinator.curG = &G{Id: 1}

	var (
		err      error
		canRetry bool
	)

	// 没有hb节点
	canRetry, err = coordinator.triggerRb(context.TODO(), 1)
	skipErr(t, err)
	skipFalse(t, canRetry)

	clearData(t)

	// 构造hb，存在激活节点的场景
	err = wrapper.put(context.TODO(), wrapper.nodeHbInstanceId(), "bar")
	skipErr(t, err)

	canRetry, err = coordinator.triggerRb(context.TODO(), 1)
	skipErr(t, err)
	skipTrue(t, canRetry)

	clearData(t)

	// state有，g id没有
	err = wrapper.put(context.TODO(), wrapper.nodeHbInstanceId(), "bar")
	skipErr(t, err)
	err = wrapper.put(context.TODO(), wrapper.nodeRbState(), "0_1")
	skipErr(t, err)

	canRetry, err = coordinator.triggerRb(context.TODO(), 1)
	skipTrue(t, canRetry)
	skipNoErr(t, err)

	clearData(t)

	// state有，g id有
	err = wrapper.put(context.TODO(), wrapper.nodeHbInstanceId(), "bar")
	skipErr(t, err)
	err = wrapper.put(context.TODO(), wrapper.nodeRbState(), "0_1")
	skipErr(t, err)
	err = wrapper.put(context.TODO(), wrapper.nodeGId(), coordinator.curG.String())
	skipErr(t, err)

	canRetry, err = coordinator.triggerRb(context.TODO(), 1)
	skipTrue(t, canRetry)
	skipErr(t, err)

	clearData(t)

	// state和g id的leaseID不一致
	err = wrapper.put(context.TODO(), wrapper.nodeHbInstanceId(), "bar")
	skipErr(t, err)
	err = wrapper.put(context.TODO(), wrapper.nodeRbState(), "0_2")
	skipErr(t, err)
	err = wrapper.put(context.TODO(), wrapper.nodeGId(), coordinator.curG.String())
	skipErr(t, err)

	canRetry, err = coordinator.triggerRb(context.TODO(), 1)
	skipTrue(t, canRetry)
	skipNoErr(t, err)

	clearData(t)

	// state没有，g id有
	err = wrapper.put(context.TODO(), wrapper.nodeHbInstanceId(), "bar")
	skipErr(t, err)
	err = wrapper.put(context.TODO(), wrapper.nodeGId(), coordinator.curG.String())
	skipErr(t, err)

	canRetry, err = coordinator.triggerRb(context.TODO(), 1)
	skipTrue(t, canRetry)
	skipNoErr(t, err)
}

func Test_leaderHandleRb_idle2revoke(t *testing.T) {
	clearData(t)

	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)
	coordinator.etcdWrapper = wrapper
	coordinator.instanceId = "testInstance"
	coordinator.curG = &G{
		Id:          1,
		Participant: []string{coordinator.instanceId},
		Timestamp:   time.Now().Unix(),
	}

	// leader涉及到
	coordinator.assignor = &ConsistentHashingAssignor{}

	worker := &workerTest{}
	assignmentParser := &assignmentParserTest{}
	coordinator.taskHub = NewTaskHub(context.TODO(), worker, assignmentParser)

	go func() {

		var err error

		// 模拟instance join revoke
		time.Sleep(3 * time.Second)
		err = wrapper.put(context.TODO(), wrapper.nodeGJoinInstance(coordinator.curG.Id), StateRevoke.String())
		skipErr(t, err)

		time.Sleep(3 * time.Second)
		stateValue := fmt.Sprintf("%s_%d", StateIdle.String(), coordinator.leaseID)
		err = wrapper.put(context.TODO(), wrapper.nodeRbState(), stateValue)
		skipErr(t, err)

	}()

	coordinator.leaderHandleRb(context.TODO())
}

func Test_leaderHandleRb_revoke2assign(t *testing.T) {
	clearData(t)

	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)
	coordinator.etcdWrapper = wrapper
	coordinator.instanceId = "testInstance"
	coordinator.curG = &G{
		Id:          1,
		Participant: []string{coordinator.instanceId},
		Timestamp:   time.Now().Unix(),
	}

	// leader涉及到
	coordinator.assignor = &ConsistentHashingAssignor{}

	worker := &workerTest{}
	assignmentParser := &assignmentParserTest{}
	coordinator.taskHub = NewTaskHub(context.TODO(), worker, assignmentParser)

	go func() {

		var err error

		// 模拟instance join revoke
		time.Sleep(3 * time.Second)
		err = wrapper.put(context.TODO(), wrapper.nodeGJoinInstance(coordinator.curG.Id), StateAssign.String())
		skipErr(t, err)

		time.Sleep(3 * time.Second)
		stateValue := fmt.Sprintf("%s_%d", StateRevoke.String(), coordinator.leaseID)
		err = wrapper.put(context.TODO(), wrapper.nodeRbState(), stateValue)
		skipErr(t, err)

	}()

	coordinator.leaderHandleRb(context.TODO())
}

func Test_waitState(t *testing.T) {
	clearData(t)

	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)
	coordinator.etcdWrapper = wrapper
	coordinator.instanceId = "testInstance"
	coordinator.curG = &G{
		Id:          1,
		Participant: []string{coordinator.instanceId},
		Timestamp:   time.Now().Unix(),
	}

	var err error

	// err = wrapper.put(context.TODO(), wrapper.nodeRbState(), StateRevoke.String())
	// skipErr(t, err)

	go func() {
		time.Sleep(3 * time.Second)

		err = wrapper.put(context.TODO(), wrapper.nodeRbState(), StateRevoke.String())
		skipErr(t, err)
	}()

	err = coordinator.waitState(context.TODO(), StateRevoke.String())
	skipErr(t, err)
}

func Test_waitInstanceState(t *testing.T) {
	clearData(t)

	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)
	coordinator.etcdWrapper = wrapper
	coordinator.instanceId = "testInstance"
	coordinator.curG = &G{
		Id:          1,
		Participant: []string{coordinator.instanceId},
		Timestamp:   time.Now().Unix(),
	}

	var err error

	go func() {
		time.Sleep(3 * time.Second)

		// 模拟instance join revoke
		err := wrapper.put(context.TODO(), wrapper.nodeGJoinInstance(coordinator.curG.Id), StateRevoke.String())
		skipErr(t, err)
	}()

	err = coordinator.waitInstanceState(context.TODO(), StateRevoke.String(), coordinator.curG.Participant)
	skipErr(t, err)
}

func Test_waitAdjustAssignment(t *testing.T) {
	fn := func(ctx context.Context, assignment string) error {
		return errors.New("err")
	}

	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}

	assignment := "foo"
	coordinator.waitAdjustAssignment(context.TODO(), assignment, fn)
}

func Test_watchG(t *testing.T) {
	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)
	coordinator.etcdWrapper = wrapper
	coordinator.instanceId = "testInstance"
	coordinator.curG = &G{Id: 1}

	worker := &workerTest{}
	assignmentParser := &assignmentParserTest{}
	taskHub := NewTaskHub(context.TODO(), worker, assignmentParser)
	coordinator.taskHub = taskHub

	coordinator.watchG(context.TODO())
}

func Test_JoinGroup(t *testing.T) {

	instanceId := "testInstance"

	config := &testTaskProvider{}
	worker := &workerTest{}
	assignmentParser := &assignmentParserTest{}
	taskHub := NewTaskHub(context.TODO(), worker, assignmentParser)

	assignor := &ConsistentHashingAssignor{}

	coordinator, err := StartWorkerCoordinator(
		context.TODO(),
		WithEtcdEndpoints([]string{"10.188.40.83:2379"}),
		WithProtocol("foo"),
		WithBiz("bar"),
		WithInstanceId(instanceId),
		WithTaskHub(taskHub),
		WithTaskProvider(config),
		WithAssignor(assignor))
	if err != nil {
		panic(err)
	}

	go func() {
		time.Sleep(10 * time.Second)

		coordinator.Close(context.TODO())
	}()

	ch := make(chan struct{})
	<-ch
}

func Test_tryDelExpiredG(t *testing.T) {
	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)
	coordinator.etcdWrapper = wrapper
	coordinator.instanceId = "testInstance"
	coordinator.curG = &G{Id: 1}

	// ./etcdctl put /rmf/foo/bar/rb/g/id/2 '{"id":7587850394640471342,"participant":["testInstance"],"timestamp":1605769549}'
	coordinator.tryCleanExpiredGDataNode(context.TODO())
}

func Test_watchHb(t *testing.T) {
	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)
	coordinator.etcdWrapper = wrapper
	coordinator.instanceId = "testInstance"
	coordinator.curG = &G{Id: 1}

	resp, err := wrapper.get(context.TODO(), wrapper.nodeHb(), []clientv3.OpOption{clientv3.WithPrefix()})
	skipErr(t, err)

	cancelCtx, cancelFunc := context.WithCancel(context.TODO())

	rev := resp.Header.Revision
	rev++
	go coordinator.watchHb(cancelCtx, rev, nil)

	go func() {
		time.Sleep(3 * time.Second)
		cancelFunc()
	}()

	time.Sleep(10 * time.Second)
}

func Test_tryStaticMembership(t *testing.T) {
	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)
	coordinator.etcdWrapper = wrapper
	coordinator.instanceId = "testInstance"
	coordinator.curG = &G{Id: 1}

	worker := &workerTest{}
	assignmentParser := &assignmentParserTest{}
	taskHub := NewTaskHub(context.TODO(), worker, assignmentParser)
	coordinator.taskHub = taskHub

	coordinator.staticMembership(context.TODO())
}

func Test_instanceHb(t *testing.T) {
	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)
	coordinator.etcdWrapper = wrapper
	coordinator.instanceId = "testInstance"
	coordinator.curG = &G{Id: 1}

	coordinator.instanceHb(context.TODO())
}

func clearData(t *testing.T) {
	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)
	coordinator.etcdWrapper = wrapper
	coordinator.instanceId = "testInstance"
	coordinator.curG = &G{Id: 1}

	var err error

	err = wrapper.del(context.TODO(), wrapper.nodeRbState())
	skipErr(t, err)
	err = wrapper.del(context.TODO(), wrapper.nodeHb())
	skipErr(t, err)
	err = wrapper.del(context.TODO(), wrapper.nodeGId())
	skipErr(t, err)
	err = wrapper.del(context.TODO(), wrapper.nodeRbLocker())
	skipErr(t, err)
}
