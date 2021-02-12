package lrmf

/*

与etcd对接，封装框架层面的r/w逻辑

etcd/clientv3与grpc、go-systemd有不兼容的情况
https://github.com/etcd-io/etcd/issues/11749
https://github.com/coreos/go-systemd/issues/321
https://github.com/etcd-io/etcd/issues/12124

etcd/clientv3使用例子
https://pkg.go.dev/go.etcd.io/etcd/clientv3

*/

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
)

const (
	defaultDialEtcdTimeout = 5
)

var (
	defaultOpTimeout = 3 * time.Second
)

type etcdWrapper struct {
	etcdClientV3 *clientv3.Client

	// 这里涉及到一个coordinator是否需要处理不同protocol的问题，不同业务场景的任务在规模大的场景下，混合均衡没有意义，放在不同cluster更加合适
	// etcdWrapper隔离掉coordinator中etcd操作的部分
	coordinator *WorkerCoordinator
}

func NewEtcdWrapper(ctx context.Context, endpoints []string, coordinator *WorkerCoordinator) (*etcdWrapper, error) {
	if len(endpoints) < 1 {
		return nil, errors.New("You must provide at least one etcd address")
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: defaultDialEtcdTimeout * time.Second,
	})
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return &etcdWrapper{etcdClientV3: client, coordinator: coordinator}, nil
}

func (w *etcdWrapper) nodePrefix() string {
	return fmt.Sprintf("%s/%s/%s", w.coordinator.protocol, w.coordinator.biz, w.coordinator.taskProvider.Tenancy())
}

func (w *etcdWrapper) nodeG() string {
	return fmt.Sprintf("/rmf/%s/rb/g/", w.nodePrefix())
}

func (w *etcdWrapper) nodeGId() string {
	return fmt.Sprintf("/rmf/%s/rb/g/id", w.nodePrefix())
}

func (w *etcdWrapper) nodeGJoin() string {
	return fmt.Sprintf("/rmf/%s/rb/g/join/", w.nodePrefix())
}

func (w *etcdWrapper) nodeGJoinId(gId int64) string {
	return fmt.Sprintf("/rmf/%s/rb/g/join/%d/", w.nodePrefix(), gId)
}

func (w *etcdWrapper) nodeGJoinInstance(gId int64) string {
	return fmt.Sprintf("/rmf/%s/rb/g/join/%d/%s", w.nodePrefix(), gId, w.coordinator.instanceId)
}

func (w *etcdWrapper) nodeGAssign() string {
	return fmt.Sprintf("/rmf/%s/rb/g/assign/", w.nodePrefix())
}

func (w *etcdWrapper) nodeGAssignId(gId int64) string {
	return fmt.Sprintf("/rmf/%s/rb/g/assign/%d/", w.nodePrefix(), gId)
}

func (w *etcdWrapper) nodeGAssignInstanceId(gId int64, instanceId string) string {
	return fmt.Sprintf("/rmf/%s/rb/g/assign/%d/%s", w.nodePrefix(), gId, instanceId)
}

func (w *etcdWrapper) nodeRbState() string {
	return fmt.Sprintf("/rmf/%s/rb/state", w.nodePrefix())
}

func (w *etcdWrapper) nodeRbLeader() string {
	return fmt.Sprintf("/rmf/%s/rb/leader", w.nodePrefix())
}

func (w *etcdWrapper) nodeHb() string {
	return fmt.Sprintf("/rmf/%s/hb/", w.nodePrefix())
}

func (w *etcdWrapper) nodeHbInstanceId() string {
	return fmt.Sprintf("/rmf/%s/hb/%s", w.nodePrefix(), w.coordinator.instanceId)
}

func (w *etcdWrapper) nodeRbLocker() string {
	return fmt.Sprintf("/rmf/%s/rb/lock", w.nodePrefix())
}

func (w *etcdWrapper) nodeTaskId(taskId string) string {
	return fmt.Sprintf("/rmf/%s/task/%s", w.nodePrefix(), taskId)
}

func (w *etcdWrapper) get(ctx context.Context, node string, opts []clientv3.OpOption) (*clientv3.GetResponse, error) {
	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	resp, err := w.etcdClientV3.Get(timeoutCtx, node, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return resp, nil
}

func (w *etcdWrapper) put(ctx context.Context, node, value string) error {
	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	if _, err := w.etcdClientV3.Put(timeoutCtx, node, value); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

// test
func (w *etcdWrapper) del(ctx context.Context, prefix string) error {
	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	resp, err := w.etcdClientV3.Delete(timeoutCtx, prefix, clientv3.WithPrefix())
	if err != nil {
		return errors.Wrap(err, "")
	}
	if resp.Deleted == 0 {
		Logger.Printf("FAILED to del %s no kv exist", prefix)
	}
	return nil
}

func (w *etcdWrapper) createAndGet(ctx context.Context, node string, value string, ttl int64) (string, clientv3.LeaseID, error) {
	// 创建的场景下，cmp只发生一次
	cmp := clientv3.Compare(clientv3.CreateRevision(node), "=", 0)

	var (
		create clientv3.Op

		leaseID clientv3.LeaseID
	)
	if ttl <= 0 {
		create = clientv3.OpPut(node, value)
	} else {
		timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
		defer cancel()

		// ttl最小5秒
		resp, err := w.etcdClientV3.Grant(timeoutCtx, ttl)
		if err != nil {
			return "", leaseID, errors.Wrap(err, "")
		}

		leaseID = resp.ID

		create = clientv3.OpPut(node, value, clientv3.WithLease(resp.ID))
	}

	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	get := clientv3.OpGet(node)
	resp, err := w.etcdClientV3.Txn(timeoutCtx).If(cmp).Then(create).Else(get).Commit()
	if err != nil {
		return "", leaseID, errors.Wrap(err, "")
	}
	if resp.Succeeded {
		Logger.Printf("Successfully create node %s with value %s", node, value)
		return value, leaseID, nil
	}
	// 创建失败，不需要再继续，业务认定自己是创建的场景，curValue不能走下面的compare and swap
	curValue := string(resp.Responses[0].GetResponseRange().Kvs[0].Value)
	Logger.Printf("FAILED to create node %s with value %s because node already exist", node, value)
	return curValue, leaseID, errNodeExist
}

// https://etcd.io/docs/v3.4.0/learning/api/#transaction
func (w *etcdWrapper) compareAndSwap(ctx context.Context, node string, curValue string, newValue string, ttl int64) error {
	_, err := w.compareAndSwapWithCurValue(ctx, node, curValue, newValue, ttl)
	return err
}

func (w *etcdWrapper) compareAndSwapWithCurValue(ctx context.Context, node string, curValue string, newValue string, ttl int64) (string, error) {
	if curValue == "" || newValue == "" {
		return "", errors.Errorf("FAILED node %s's curValue or newValue should not be empty", node)
	}

	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	var put clientv3.Op
	if ttl <= 0 {
		put = clientv3.OpPut(node, newValue)
	} else {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), defaultOpTimeout)
		defer cancel()

		resp, err := w.etcdClientV3.Grant(timeoutCtx, ttl)
		if err != nil {
			return "", errors.Wrap(err, "")
		}

		put = clientv3.OpPut(node, newValue, clientv3.WithLease(resp.ID))
	}

	// leader会尝试保持自己的状态
	cmp := clientv3.Compare(clientv3.Value(node), "=", curValue)
	get := clientv3.OpGet(node)
	resp, err := w.etcdClientV3.Txn(timeoutCtx).If(cmp).Then(put).Else(get).Commit()
	if err != nil {
		return "", errors.Wrapf(err, "FAILED to swap node %s from %s to %s", node, curValue, newValue)
	}
	if resp.Succeeded {
		Logger.Printf("Successfully swap node %s from %s to %s", node, curValue, newValue)
		return "", nil
	}
	if resp.Responses[0].GetResponseRange().Count == 0 {
		return "", errors.Errorf("FAILED to swap node %s, node not exist, but want change value from %s to %s", node, curValue, newValue)
	}
	realValue := string(resp.Responses[0].GetResponseRange().Kvs[0].Value)
	if realValue == newValue {
		Logger.Printf("FAILED to swap node %s, current value %s, but want change value from %s to %s", node, realValue, curValue, newValue)
		return realValue, errNodeValueAlreadyExist
	}
	Logger.Printf("FAILED to swap node %s, current value %s, but want change value from %s to %s", node, realValue, curValue, newValue)
	return realValue, errNodeValueErr
}

// https://github.com/etcd-io/etcd/issues/9714
func (w *etcdWrapper) incrementAndGet(ctx context.Context, node string) (int, error) {
	resp, err := w.get(ctx, node, nil)
	if err != nil {
		return -1, errors.Wrap(err, "")
	}

	if resp.Count == 0 {
		if _, _, err := w.createAndGet(context.TODO(), node, "1", -1); err != nil {
			return -1, errors.Wrap(err, "")
		}
		return 1, nil
	} else {
		value := string(resp.Kvs[0].Value)
		valueInt, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return -1, errors.Wrap(err, "")
		}

		newValue := valueInt + 1
		if err := w.compareAndSwap(context.TODO(), node, value, strconv.FormatInt(newValue, 10), -1); err != nil {
			return -1, errors.Wrap(err, "")
		}
		return int(newValue), nil
	}
}

func (w *etcdWrapper) getKVs(ctx context.Context, prefix string) (map[string]string, error) {
	// https://github.com/etcd-io/etcd/blob/master/tests/integration/clientv3/kv_test.go
	opts := []clientv3.OpOption{clientv3.WithPrefix()}
	resp, err := w.get(ctx, prefix, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "FAILED to get prefix %s", prefix)
	}
	if resp.Count == 0 {
		return nil, nil
	}

	r := make(map[string]string)
	for _, kv := range resp.Kvs {
		_, file := filepath.Split(string(kv.Key))
		r[file] = string(kv.Value)
	}
	return r, nil
}

func (w *etcdWrapper) getRbInstanceIdAndJoin(ctx context.Context) (map[string]string, error) {
	return w.getKVs(ctx, w.nodeGJoinId(w.coordinator.newG.Id))
}

func (w *etcdWrapper) getRbInstanceIdAndAssign(ctx context.Context) (map[string]string, error) {
	return w.getKVs(ctx, w.nodeGAssignId(w.coordinator.newG.Id))
}

func (w *etcdWrapper) getHbInstanceIds(ctx context.Context) ([]string, error) {
	prefix := w.nodeHb()
	opts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithKeysOnly()}
	resp, err := w.get(ctx, prefix, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "FAILED to get prefix %s", prefix)
	}
	if resp.Count == 0 {
		return nil, nil
	}

	var r []string
	for _, kv := range resp.Kvs {
		_, file := filepath.Split(string(kv.Key))
		r = append(r, file)
	}
	return r, nil
}

func (w *etcdWrapper) getState(ctx context.Context) (string, error) {
	resp, err := w.get(ctx, w.nodeRbState(), nil)
	if err != nil {
		return "", errors.Wrap(err, "")
	}
	if resp.Count == 0 {
		return "", errNotNodeExist
	}
	return string(resp.Kvs[0].Value), nil
}

// https://tangxusc.github.io/blog/2019/05/etcd-lock%E8%AF%A6%E8%A7%A3/
// https://github.com/etcd-io/etcd/blob/master/tests/integration/clientv3/concurrency/example_mutex_test.go
// https://github.com/etcd-io/etcd/blob/master/Documentation/learning/lock/client/client.go
func (w *etcdWrapper) acquireLock(ctx context.Context, ttl int) (locker sync.Locker, leaseID clientv3.LeaseID, err error) {

	defer func() {
		if rerr := recover(); rerr != nil {
			var buf [4096]byte
			n := runtime.Stack(buf[:], false)
			Logger.Printf(fmt.Sprintf("panic because err %+v", rerr))
			Logger.Printf("%s", string(buf[:n]))

			err = errors.Errorf("acquire lock failed")
		}
	}()

	// ttl表示其他节点可以重新尝试操作存储key（大部分场景都能在这个ttl完成，过期是方式deadlock）
	// 在ttl内rmf需要改变rb state，开启新g（增加revoke节点），默认给10s
	session, err := concurrency.NewSession(w.etcdClientV3, concurrency.WithTTL(ttl))
	if err != nil {
		return nil, 0, errors.Wrap(err, "")
	}

	locker = concurrency.NewLocker(session, w.nodeRbLocker())
	// 等defaultLeaderLockSessionTimeout会超时
	locker.Lock()
	return locker, session.Lease(), nil
}

func (w *etcdWrapper) campaign(ctx context.Context, ttl int) (*concurrency.Session, *concurrency.Election, error) {
	// 有无限keepalive的保证，所以一般情况下，leader不会更改，重启的时候可能会导致leader重新选举
	s, err := concurrency.NewSession(w.etcdClientV3, concurrency.WithTTL(ttl))
	if err != nil {
		return nil, nil, errors.Wrap(err, "")
	}

	e := concurrency.NewElection(s, w.nodeRbLeader())
	if err := e.Campaign(ctx, w.coordinator.instanceId); err != nil {
		return nil, nil, errors.Wrap(err, "")
	}

	Logger.Printf("Election success for %s", w.coordinator.instanceId)
	return s, e, nil
}
