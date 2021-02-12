package lrmf

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
)

func Test_putAndget(t *testing.T) {
	coordinator := &WorkerCoordinator{}
	wrapper, err := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	if err != nil {
		t.Errorf("%+v", err)
		t.SkipNow()
	}

	if err := wrapper.put(context.TODO(), "foo", "bar"); err != nil {
		t.Errorf("%+v", err)
		t.SkipNow()
	}

	getResponse, err := wrapper.get(context.TODO(), "foo", nil)
	if err != nil {
		t.Errorf("%+v", err)
		t.SkipNow()
	}

	if getResponse.Count != 1 {
		t.Error("count should be 1")
		t.SkipNow()
	}

	fmt.Println(string(getResponse.Kvs[0].Key))
	fmt.Println(string(getResponse.Kvs[0].Value))

	if string(getResponse.Kvs[0].Value) != "bar" {
		t.Error("value should be bar")
		t.SkipNow()
	}
}

func Test_createAndGet(t *testing.T) {
	coordinator := &WorkerCoordinator{}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)

	var (
		value string
		err   error
	)

	// etcd没有foo
	_ = wrapper.del(context.TODO(), "foo")
	value, _, err = wrapper.createAndGet(context.TODO(), "foo", "bar", -1)
	skipErr(t, err)

	// etcd有foo
	err = wrapper.put(context.TODO(), "foo", "bar")
	skipErr(t, err)
	value, _, err = wrapper.createAndGet(context.TODO(), "foo", "bar", -1)
	if err != errNodeExist {
		t.Errorf("expect errNodeExist")
		t.SkipNow()
	}

	// ttl
	_ = wrapper.del(context.TODO(), "foo")
	value, _, err = wrapper.createAndGet(context.TODO(), "foo", "bar", 5)
	skipErr(t, err)
	getResp, gerr := wrapper.get(context.TODO(), "foo", nil)
	skipErr(t, gerr)
	if getResp.Count != 1 || string(getResp.Kvs[0].Value) != "bar" {
		t.Error("foo's value should be bar")
		t.SkipNow()
	}
	time.Sleep(5 * time.Second)
	getResp2, gerr2 := wrapper.get(context.TODO(), "foo", nil)
	skipErr(t, gerr2)
	if getResp2.Count > 0 {
		t.Error("foo should be expire")
		t.SkipNow()
	}

	fmt.Println(value, err)
}

func Test_compareAndSwap(t *testing.T) {
	coordinator := &WorkerCoordinator{}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)

	var err error

	// etcd存在foo
	err = wrapper.put(context.TODO(), "foo", "bar")
	skipErr(t, err)

	// curValue正确场景
	err = wrapper.compareAndSwap(context.TODO(), "foo", "bar", "bar2", -1)
	skipErr(t, err)

	// curValue错误场景
	err = wrapper.compareAndSwap(context.TODO(), "foo", "bar", "bar2", -1)
	if err == nil {
		t.Errorf("current is bar2, should not swap")
		t.SkipNow()
	}

	// foo不存在场景
	_ = wrapper.del(context.TODO(), "foo")
	err = wrapper.compareAndSwap(context.TODO(), "foo", "bar", "bar2", -1)
	if err == nil {
		t.Errorf("current value not exist, should not ok")
		t.SkipNow()
	}

	// ttl
	err = wrapper.put(context.TODO(), "foo", "bar")
	err = wrapper.compareAndSwap(context.TODO(), "foo", "bar", "bar2", 30)
	skipErr(t, err)
	time.Sleep(5 * time.Second)
	getResp, gerr := wrapper.get(context.TODO(), "foo", nil)
	skipErr(t, gerr)
	if getResp.Count > 0 {
		t.Error("foo should be expire")
		t.SkipNow()
	}
}

func Test_getKVs(t *testing.T) {
	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)

	var err error

	prefix := wrapper.nodeHb()
	_ = wrapper.del(context.TODO(), prefix)

	err = wrapper.put(context.TODO(), prefix+"/1", "1")
	skipErr(t, err)
	err = wrapper.put(context.TODO(), prefix+"/2", "2")
	skipErr(t, err)

	m, err := wrapper.getKVs(context.TODO(), prefix)
	skipErr(t, err)
	if len(m) != 2 {
		t.Errorf("count err %+v", m)
		t.SkipNow()
	}
}

func Test_incrementAndGet(t *testing.T) {
	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)

	var (
		value int
		err   error
	)

	node := "/counter"
	_ = wrapper.del(context.TODO(), node)

	value, err = wrapper.incrementAndGet(context.TODO(), node)
	skipErr(t, err)
	if value != 1 {
		t.Errorf("value should be 1 but %d", value)
		t.SkipNow()
	}

	value, err = wrapper.incrementAndGet(context.TODO(), node)
	skipErr(t, err)
	if value != 2 {
		t.Errorf("value should be 2 but %d", value)
		t.SkipNow()
	}

	// concurrent
	// incrementAndGet能防止counter被覆盖，全局多个leader的场景在网络被隔离的场景可能发生，
	// 如果不能开启g，需要不断尝试，最终会触发两次rb，前一次rb会异常终止，但最终任务会rb到ok
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
		retry:
			value, err = wrapper.incrementAndGet(context.TODO(), node)
			if err != nil {
				fmt.Println(err)
				time.Sleep(300 * time.Millisecond)
				goto retry
			}
		}()
	}
	wg.Wait()
	if value != 7 {
		t.Errorf("value should be 7 but %d", value)
		t.SkipNow()
	}
}

func Test_acquireLock(t *testing.T) {
	// 检验leaseID是否是全局递增的
	coordinator := &WorkerCoordinator{protocol: "foo", biz: "bar"}
	wrapper, werr := NewEtcdWrapper(context.TODO(), []string{"10.188.40.83:2379"}, coordinator)
	skipErr(t, werr)

	var (
		locker  sync.Locker
		leaseID clientv3.LeaseID
		err     error
	)

	locker, leaseID, err = wrapper.acquireLock(context.TODO(), 10)
	skipErr(t, err)
	locker.Lock()
	locker.Unlock()

	fmt.Println(leaseID)

	locker, leaseID, err = wrapper.acquireLock(context.TODO(), 10)
	skipErr(t, err)
	locker.Lock()
	locker.Unlock()

	fmt.Println(leaseID)
}

func skipErr(t *testing.T, err error) {
	if err != nil {
		t.Errorf("%+v", err)
		t.SkipNow()
	}
}

func skipNoErr(t *testing.T, err error) {
	if err == nil {
		t.Errorf("%+v", err)
		t.SkipNow()
	}
}

func skipTrue(t *testing.T, value bool) {
	if value {
		t.Errorf("%t", value)
		t.SkipNow()
	}
}

func skipFalse(t *testing.T, value bool) {
	if !value {
		t.Errorf("%t", value)
		t.SkipNow()
	}
}
