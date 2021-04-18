package lrmf

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
)

type instanceState int

const (
	StateIdle instanceState = iota
	StateRevoke
	StateAssign
)

func (s instanceState) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateRevoke:
		return "revoke"
	case StateAssign:
		return "assign"
	}
	panic(fmt.Sprintf("Unexpected state %d", s))
}

const (
	// etcd中hb node过期时间，对于hb的容忍限度增加，降低rb的频率，增加问题时长，一般15s可接受（sarama默认），这里只能说10分钟不可接受，但不能精确到s级别
	defaultSessionTimeout = 15

	defaultRbTimeout int64 = 30

	// 出错重试需要等一会，这里设定默认值
	// kafka中有很多timeout设定，确认的是分钟级别肯定是接受不了
	defaultOpWaitTimeout = 1 * time.Second
)

type Coordinator struct {
	// 例如：Kafka
	protocol string
	// service name，用于对接naming service
	biz string
	// https://www.confluent.io/blog/kafka-rebalance-protocol-static-membership/#unnecessary-rebalance
	// 客户端提供用于标记资源个体，coordinator发现已经assign过任务，直接返回当前记录的历史任务，在blog中描述的场景不需要触发rebalance。
	// 触发rebalance的场景：
	// https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances
	// The reason for triggering rebalance when leader rejoins is because there might be assignment protocol change (for example if the consumer group is using regex subscription and new matching topics show up).
	// leader rejoin的场景需要rebalance，这也会发生在rolling bounces场景下，但是static membership会控制其他节点启动不会触发rebalance。
	instanceId string

	// 基于etcd机制实现
	etcdWrapper *etcdWrapper

	// 轮次，标记当前instance所处的generation，在etcd中g的存储空间通过目录隔离，instance在g的空间内协作完成任务分配
	curG *G

	// 与worker之间的通道
	taskHub TaskHub

	// 资源分配映射策略
	assignor Assignor

	// leader获取任务的通道
	taskProvider TaskProvider

	// 管理coordinator内部goroutine：
	// 1. leaderCamp leader竞选，选中后处理整体rb过程中的协调工作
	// 2. hb instance保证自己存活的机制，利用clientv3提供的机制与etcd做交互
	// 3. watchG instance关注rb事件，leader关注instance存活(在leaderCamp中)，只有leader能够触发rb
	gracefulCancelFunc context.CancelFunc
	gracefulWG         sync.WaitGroup

	// 同一WorkerCoordinator对象不允许多次join group，框架无bug场景，会自修复
	mu sync.Mutex

	// 不允许JoinGroup重复进入
	joined bool

	// rb过程中leader需要利用leaseID防止split brain，作用在state节点
	newG *G // 4 leader

	// 利用etcd中lease机制管理与特定instance相关的任务节点，一旦instance因为网络或者自身原因不能继续保证lease的存活，与该lease
	// 相关的任务节点都会被释放。
	// 任务与instance存活相关，instance存活的标记在etcd中是通过hb节点保证的，所以和hb使用同样的lease。
	// 注意：
	// 1. instance之于etcd不是存活状态，不代表instance所处的进程已经发起被分配的工作，lrmf作为辅助库不能干扰接入应用的行为。
	// 2. kafka在该场景下，broker是知道某个instance不是存活状态，可以禁止它继续拉取消息/标记offset，但是lrmf作为第三方的库，做不到。
	instanceLeaseID clientv3.LeaseID
}

func (c *Coordinator) JoinGroup(ctx context.Context) error {
	/*
		rebalance触发时机：
		1 worker增加减少（这块需要考虑rolling bounce，引入static membership机制）
		2 leader和worker一样，只是leader的leave相比worker代价更大（leader需要关注instance的hb、也要关注g）
		3 etcd故障不影响任务处理，但在故障期间没有rebalance机制，重启instance会导致业务故障
		4 mq特定场景下，partition增加（这个需要具体集成的业务场景自己保证，任务的变更需要提供接口给leader）

		rmf只关注资源（一般是进程资源）的变动
	*/

	// 防止业务层在同一coordinator上调用多次
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.joined {
		return errors.New("FAILED to call join group, already running")
	}

	// leader/follower角色可能在运行过程中变化，单独有cancel，其他goroutine都是随着coordinator生命周期的，可以对外提供Close方法
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	c.gracefulCancelFunc = cancelFunc

	c.gracefulWG.Add(3)
	go withRecover(cancelCtx, c.leaderCamp)
	go withRecover(cancelCtx, c.watchG)

	return nil
}

func (c *Coordinator) Close(ctx context.Context) {
	if c.gracefulCancelFunc != nil {
		c.gracefulCancelFunc()
		c.gracefulWG.Wait()
	}
	Logger.Printf("coordinator %s exit on g %s", c.instanceId, c.curG.String())
}

func (c *Coordinator) TriggerRb(ctx context.Context) error {
	if err := c.tryTriggerRb(ctx); err != nil {
		return errors.Wrap(err, "FAILED to TriggerRb")
	}
	return nil
}

type G struct {
	// generation的Id，和instanceId区分开
	// 选择int类型，对于generation的操作必须是顺序的，不能回退
	Id int64 `json:"id"`

	// 限定参与此次rb的instance
	Participant []string `json:"participant"`

	// 开始时间，用于计算rb timeout
	Timestamp int64 `json:"timestamp"`
}

func (g *G) String() string {
	b, _ := json.Marshal(g)
	return string(b)
}

func (g *G) LeaseID() clientv3.LeaseID {
	return clientv3.LeaseID(g.Id)
}

func ParseG(ctx context.Context, val string) *G {
	g := G{}
	if err := json.Unmarshal([]byte(val), &g); err != nil {
		// 有些错误不可能发生，发生就可以直接panic，没必要让程序继续执行
		panic(fmt.Sprintf("Unexpect err:  FAILED to unmarshal %s, err %s", val, err.Error()))
	}
	return &g
}

func (c *Coordinator) leaderCamp(ctx context.Context) {
	// 参考文档：https://github.com/entertainment-venue/lrmf/wiki/etcd-clientv3%E7%AB%9E%E4%BA%89leader%E6%9C%BA%E5%88%B6
	defer c.gracefulWG.Done()

	for {
	tryCampaign:
		select {
		case <-ctx.Done():
			Logger.Printf("leaderCamp exit, when tryCampaign")
			return
		default:
		}

		// 有无限keepalive的保证，所以一般情况下，leader不会更改，重启的时候可能会导致leader重新选举
		session, err := concurrency.NewSession(c.etcdWrapper.etcdClientV3, concurrency.WithTTL(defaultSessionTimeout))
		if err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultOpWaitTimeout)
			goto tryCampaign
		}

		election := concurrency.NewElection(session, c.etcdWrapper.nodeRbLeader())
		if err := election.Campaign(ctx, c.instanceId); err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultOpWaitTimeout)
			goto tryCampaign
		}

		Logger.Printf("Successfully campaign for current instance %s", c.instanceId)

		// 防止leaderHandleRb到watchHb之间加入/删除的instance被漏掉，这里先取到hb节点的revision
	fetchStartRevision:
		select {
		case <-ctx.Done():
			Logger.Printf("leaderCamp exit, when fetchStartRevision")
			return
		default:
		}

		resp, err := c.etcdWrapper.get(ctx, c.etcdWrapper.nodeRbLeader(), []clientv3.OpOption{clientv3.WithPrefix()})
		if err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultOpWaitTimeout)
			goto fetchStartRevision
		}
		startRevision := resp.Header.Revision

		// 再次调用campaign会以新的身份block，所以一定要是s.Done()
	tryTriggerRb:
		select {
		case <-session.Done():
			Logger.Printf("leader session done, tryCampaign again")
			goto tryCampaign
		case <-ctx.Done():
			if err := session.Close(); err != nil {
				Logger.Printf("err %+v", err)
			}
			return
		default:
		}

		if err := c.tryTriggerRb(ctx); err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultOpWaitTimeout)
			goto tryTriggerRb
		}

		// 开启rb监管goroutine
		if err := c.leaderHandleRb(ctx); err != nil {
			if !errors.Is(err, errClose) {
				Logger.Printf("Unexpected err: %+v", err)
			}

			time.Sleep(defaultOpWaitTimeout)
			goto tryTriggerRb
		} else {
			Logger.Printf("leader handle rb completed g [%s]", c.newG.String())
		}

		// leader需要检查rb，监听hb
		if err := c.watchInstances(ctx, startRevision, session.Done()); err != nil {
			Logger.Printf("Leader watch hb err: %+v", err)
		}

		// watchHb会block在s.Done()，执行到这里可以触发一次清理
		if err := election.Resign(ctx); err != nil {
			Logger.Printf("FAILED to resign, err: %+v", err)
		}
	}
}

func (c *Coordinator) watchG(ctx context.Context) {
	defer c.gracefulWG.Done()

	Logger.Print("Start watch g")

	// 如果拿到的g在assign完成后，已经不是最新的g，下面的Watch会参与到后续rb中；
	// 拿到g后，会获取当前state，如果是rb，就认为当前g不合法，尝试参与到g之后的rb event中。
tryStatic:
	// 使用goto的场景都需要做done的判断，防止因为etcd挂掉导致goroutine导致不能接受主动退出指令
	select {
	case <-ctx.Done():
		Logger.Printf("watchG exit when tryStatic")
		return
	default:
	}

	rev, err := c.staticMembership(ctx)
	if err != nil {
		Logger.Printf("FAILED to static, err: %+v", err)
		time.Sleep(defaultOpWaitTimeout)
		goto tryStatic
	}

	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithFilterDelete())
	opts = append(opts, clientv3.WithRev(rev))

	var (
		wg         sync.WaitGroup
		cancelFunc context.CancelFunc
	)
	_, cancelFunc = context.WithCancel(context.TODO())

tryWatch:
	wch := c.etcdWrapper.etcdClientV3.Watch(ctx, c.etcdWrapper.nodeGId(), opts...)
	for {
		select {
		case <-ctx.Done():
			Logger.Printf("watchG exit")

			// watchG退出，尝试停掉自己的子goroutine
			cancelFunc()
			wg.Wait()

			return
		case wr := <-wch:
			if err := wr.Err(); err != nil {
				Logger.Printf("FAILED to watchG, err: %+v", err)

				// bugfix: g是很久之前创建的，不经常rb，新启动的节点从last create开始，理论上不需要多久就能找到rev
				if err.Error() == "etcdserver: mvcc: required revision has been compacted" {
					rev++
				}
				goto tryWatch
			}

			var (
				rb   bool
				gRaw []byte
			)
			for _, ev := range wr.Events {
				rev = ev.Kv.ModRevision + 1
				rb = true
				gRaw = ev.Kv.Value
				Logger.Printf("Got watchG ev [%s]", ev.Kv.String())
			}

			if rb {
				Logger.Printf("Use g %s", string(gRaw))

				newG := G{}
				if err := json.Unmarshal(gRaw, &newG); err != nil {
					// 只有triggerRb用到的g id node存储G类型的json string，其他直接忽律
					Logger.Printf("FAILED to unmarshal value %s, may be not rb event, err %s", gRaw, err)
					continue
				}

				// 新rb过来，强制停止，并开启新的rb操作
				cancelFunc()
				wg.Wait()

				// g的替换要等带rb goroutine回收完毕
				c.curG = &newG

				var cancelCtx context.Context
				cancelCtx, cancelFunc = context.WithCancel(context.TODO())
				wg.Add(1)
				go c.handleRbEvent(cancelCtx, &wg)
			}
		}
	}
}

func (c *Coordinator) staticMembership(ctx context.Context) (int64, error) {
	/*
		未知错误，交给上游重试，如果放弃：当前集群是idle状态，长时间某些p是不被消费的，会触发lag报警
	*/
	gNode := c.etcdWrapper.nodeGId()

	var (
		rev  int64 = -1
		opts []clientv3.OpOption
	)
	opts = append(opts, clientv3.WithLastRev()...)
	resp, err := c.etcdWrapper.get(ctx, gNode, opts)
	if err != nil {
		return -1, errors.Wrapf(err, "FAILED to get %s, err %+v", gNode, err)
	}
	rev = resp.Header.Revision
	if resp.Count == 0 {
		Logger.Printf("g empty, return ASAP")
		return rev, nil
	}

	// 如果最新的g包含当前instance的id，尝试static membership；
	// 如果获取g后，leader立即触发rb生成新g，通过task节点防止任务并行出现，等待下次rb。
	g := G{}
	gValue := string(resp.Kvs[0].Value)
	if err := json.Unmarshal([]byte(gValue), &g); err != nil {
		return -1, errors.Errorf("FAILED to unmarshal, err: %+v value: %s", err, gValue)
	}
	var exist bool
	for _, p := range g.Participant {
		if p == c.instanceId {
			exist = true
			break
		}
	}
	if !exist {
		// 不包含当前instance，handleRbEvent从下一个g开始
		Logger.Printf("instance %s not exist in participant %+v", c.instanceId, g.Participant)
		rev++
		return rev, nil
	}

	/*
		rb中，放弃当前static membership，交给handleRbEvent处理/不处理最后一个g，可能出现以下情况:
		1 这里获取的g和state不是一组（state的leaseID不是g的Id），从下一个event开始监听，不参与当前rb
		2 与1不同，从当前rev开始，需要处理当前g对应的最后g
	*/
	rawState, err := c.etcdWrapper.getState(ctx)
	if err != nil {
		return -1, errors.Wrapf(err, "FAILED to get state, err %+v", err)
	}
	state, leaseID := stateValue(rawState).StateAndLeaseID()
	if leaseID != g.Id {
		if leaseID > g.Id {
			// 在获取g之后有新g产生，交给handleRbEvent处理
			Logger.Printf("Maybe new g with state %s, latest g %s", rawState, g.String())

			// 有新g，应该从下一个g开始处理，虽然下一个g可能也不是最新的g，目标是尽快达到balance
			rev++
			return rev, nil
		}

		// leaderID是过去的，从当前g开始尝试参与rb；triggerRb中state先修改，g后新增，理论上不可能
		// TODO 人工介入
		Logger.Printf("Error leaseID in state %s, latest g %s", rawState, g.String())
		return rev, nil
	} else {
		if state != StateIdle.String() {
			Logger.Printf("Latest g %s rebalancing, try to handle", g.String())
			// 最后的g可能正在rb，尝试加入
			return rev, nil
		}

		// 当前可以信任的g，但也可能leader立即触发一次rb，我们记录的rev，可以识别到后续的rb，并会要求当前instance加入进去
	}

	assignNode := c.etcdWrapper.nodeGAssignInstanceId(g.Id, c.instanceId)
	gResp, gErr := c.etcdWrapper.get(ctx, assignNode, nil)
	if gErr != nil {
		return -1, errors.Wrapf(err, "FAILED to get %s, err %+v", assignNode, err)
	}
	if gResp.Count == 0 {
		// 先取g，再取state为idle，证明g是稳定版，但发现没有assign节点，可能g已经被新rb，清理掉，从下一个rb尝试加入即可
		rev++
		return rev, nil
	}

	assignment := string(gResp.Kvs[0].Value)
	if err := c.assign(ctx, assignment); err != nil {
		// 这里可能是因为正在rb，导致部分assign失败，会导致当前rb处理失败，进入下次rb
		return -1, errors.Wrapf(err, "FAILED to assign %s, err: %+v", assignment, err)
	}

	c.curG = &g
	// 从下一个节点开始监听
	rev++
	return rev, nil
}

func (c *Coordinator) handleRbEvent(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	/*
		follower判断是否Participant是否包含自己
		1 包含，直接rb
		2 不包含，考虑下面两种情况：
		如果是新instance（忽略，等leader通过hb识别到有可用instance触发rb即可）；
		运行任务中，hb发出delay，watchg及时，停掉现有任务，依赖task节点防止任务并行运行。
	*/

	var exist bool
	for _, instanceId := range c.curG.Participant {
		if instanceId == c.instanceId {
			exist = true
			break
		}
	}
	if !exist {
		Logger.Printf("FAILED to find instanceId %s in g's Participant %+v", c.instanceId, c.curG.Participant)

		// 需要保证revoke成功，否则任务的冲突会被检测到
		c.waitAdjustAssignment(ctx, "", c.revoke)
		return
	}

	/*
		revoke -> assign 两个阶段在全局必须串行
	*/

	if err := c.waitState(ctx, StateRevoke.String()); err != nil {
		if !errors.Is(err, errClose) {
			Logger.Printf("Unexpected err: %+v", err)
		}
		return
	}
	Logger.Printf("Instance %s waitState success %s", c.instanceId, StateRevoke)

	if err := c.instanceHandleRb(ctx, StateRevoke.String()); err != nil {
		if !errors.Is(err, errClose) {
			Logger.Printf("Unexpected err: %+v", err)
		}
		return
	}
	Logger.Printf("Instance %s instanceHandleRb completed %s", c.instanceId, StateRevoke)

	if err := c.waitState(ctx, StateAssign.String()); err != nil {
		if !errors.Is(err, errClose) {
			Logger.Printf("Unexpected err: %+v", err)
		}
		return
	}
	Logger.Printf("Instance %s waitState success %s", c.instanceId, StateAssign)

	if err := c.instanceHandleRb(ctx, StateAssign.String()); err != nil {
		if !errors.Is(err, errClose) {
			Logger.Printf("Unexpected err: %+v", err)
		}
		return
	}
	Logger.Printf("Instance %s instanceHandleRb completed %s", c.instanceId, StateAssign)

	if err := c.waitCompareAndSwap(
		ctx,
		c.curG,
		c.etcdWrapper.nodeGJoinInstance(c.curG.Id),
		StateAssign.String(),
		StateIdle.String()); err != nil {
		if !errors.Is(err, errClose) {
			Logger.Printf("Unexpected err: %+v", err)
		}
		return
	}
	Logger.Printf("rb completed on %s, g [%s]", c.instanceId, c.curG.String())
}

func (c *Coordinator) leaderHandleRb(ctx context.Context) error {
	/*
		leader干活的开始点有两个：
		1 triggerRb中因为Hb某个节点的增加/减少，instance node有变化且当前不处于rb状态
		2 leaderWatcher发现leader下线触发recognizeRole，leader重新上线触发rb
	*/

	participant := c.newG.Participant

	tasks, err := c.taskProvider.Tasks(ctx)
	if err != nil {
		return errors.Wrap(err, "FAILED to get tasks")
	}
	instanceIdAndTasks, err := c.assignor.PerformAssignment(ctx, tasks, participant)
	if err != nil {
		// 分配任务如果失败，直接return等待rb timeout
		return errors.Wrap(err, "FAILED to perform assignment")
	}
	// 新任务结果写入assign node
	for instanceId, tasks := range instanceIdAndTasks {
		b, err := json.Marshal(tasks)
		if err != nil {
			return errors.Wrapf(err, "FAILED to marshal tasks %+v", tasks)
		}
		newAssignment := string(b)

		assignNode := c.etcdWrapper.nodeGAssignInstanceId(c.newG.Id, instanceId)
	tryPut:
		if err := c.etcdWrapper.put(ctx, assignNode, newAssignment); err != nil {
			err := errors.Wrapf(err, "FAILED to put %s to %s", newAssignment, assignNode)
			Logger.Printf("err: %+v", err)

			select {
			case <-ctx.Done():
				Logger.Printf("leaderHandleRb tryPut exit")
				return err
			default:
			}

			time.Sleep(defaultOpWaitTimeout)
			goto tryPut
		}
		Logger.Printf("Successfully add assignment %s to node %s", newAssignment, assignNode)
	}

	state := StateIdle
	for {
		select {
		case <-ctx.Done():

			Logger.Printf("Instance %s leaderHandleRb exit", c.instanceId)
			return errors.Wrap(errClose, "")

		case <-time.After(defaultOpWaitTimeout):

			switch state {

			case StateIdle:

				// rb的state从idle变为revoke（表示g/join/gid/开始接收revoke），等待instance都变为revoke
				if err := c.waitCompareAndSwap(
					ctx,
					c.newG,
					c.etcdWrapper.nodeRbState(),
					formatStateValue(StateIdle.String(), c.newG.LeaseID()),
					formatStateValue(StateRevoke.String(), c.newG.LeaseID())); err != nil {
					return err
				}

				if err := c.waitInstanceState(ctx, StateRevoke.String(), participant); err != nil {
					return err
				}

				state = StateRevoke

			case StateRevoke:

				// 所有instance都加入revoke后，让集群的rb state过渡到assign状态
				if err := c.waitCompareAndSwap(
					ctx,
					c.newG,
					c.etcdWrapper.nodeRbState(),
					formatStateValue(StateRevoke.String(), c.newG.LeaseID()),
					formatStateValue(StateAssign.String(), c.newG.LeaseID())); err != nil {
					return errors.Wrap(err, "Quit waitCompareAndSwap, from revoke to assign")
				}

				// 等待instance都变为idle
				if err := c.waitInstanceState(ctx, StateIdle.String(), participant); err != nil {
					return errors.Wrap(err, "Quit waitInstanceState when waiting idle")
				}

				// rb state恢复idle状态
				if err := c.waitCompareAndSwap(
					ctx,
					c.newG,
					c.etcdWrapper.nodeRbState(),
					formatStateValue(StateAssign.String(), c.newG.LeaseID()),
					formatStateValue(StateIdle.String(), c.newG.LeaseID())); err != nil {
					return errors.Wrap(err, "Quit waitCompareAndSwap from assign to idle")
				}

				return nil
			}
		}
	}
}

func (c *Coordinator) instanceHandleRb(ctx context.Context, curState string) error {
	assignNode := c.etcdWrapper.nodeGAssignInstanceId(c.curG.Id, c.instanceId)
	resp, err := c.etcdWrapper.get(ctx, assignNode, nil)
	if err != nil {
		return errors.Wrapf(err, "FAILED to get %s", assignNode)
	}

	var (
		gotAssignment bool
		assignment    string
	)

	if resp.Count == 0 {
		// 没有给当前instance分配任务，可以直接成功，不耽误leader rb
		Logger.Printf("%s got no assignment", assignNode)
	} else {
		gotAssignment = true
		assignment = string(resp.Kvs[0].Value)
	}

	joinNode := c.etcdWrapper.nodeGJoinInstance(c.curG.Id)

	firstTry := true
joinOp:
	select {
	case <-ctx.Done():
		Logger.Printf("Instance %s instanceHandleRb exit when join revoke", c.instanceId)
		return errors.Wrap(errClose, "")
	default:
		if !firstTry {
			time.Sleep(defaultOpWaitTimeout)
		}
	}
	firstTry = false

	switch curState {
	case StateRevoke.String():

		// revoke阶段，join group在新g中相应instance下创建revoke node，leader收集第一阶段的join group请求
		// 不应该存在revoke node，存在当前follower join group失败，重新尝试加入，当前follower是leader认为需要收集到的instance，加入不进去，就要等rb timeout了

		if gotAssignment {
			// 进入revoke后，开始根据assignment revoke自己的任务
			c.waitAdjustAssignment(ctx, assignment, c.revoke)
		}

		// 本地revoke结束，标记当前instance revoke完结
		curValue, err := c.etcdWrapper.createAndGet(ctx, joinNode, StateRevoke.String(), clientv3.NoLease)
		if err != nil {
			if err != errNodeExist {
				Logger.Printf("FAILED to createAndGet %s, unexpected err %+v", joinNode, err)
				goto joinOp
			}

			if curValue != StateRevoke.String() {
				// 这种场景放弃，等leader任务当前g的rb失败（长时间收集不到所有join group），发起下一轮次，不应该除非出现bug 或者 etcd交互异常
				// 当前instanceId节点应该只有一个goroutine操作，状态不对证明有其他goroutine操作，bug导致，需要人工介入修复
				return errors.Errorf("FAILED to createAndGet %s, unexpected err %+v, quit join revoke", joinNode, err)
			}

			// 等值场景，直接继续执行，任务分配保证幂等，重复分配没有问题即可
		}

		Logger.Printf("Successfully join revoke, instance %s", c.instanceId)

		return nil

	case StateAssign.String():

		if gotAssignment {
			c.waitAdjustAssignment(ctx, assignment, c.assign)
		}

		// instance的状态从revoke变为assign
		if err := c.etcdWrapper.compareAndSwap(ctx, joinNode, StateRevoke.String(), StateAssign.String(), -1); err != nil {
			if err == errNodeValueErr {
				return errors.Errorf("FAILED to compareAndSwap %s from revoke to assign, unexpected err %s, quit join assign", joinNode, err)
			}

			if err != errNodeValueAlreadyExist {
				Logger.Printf("FAILED to compareAndSwap %s from revoke to assign , unexpected err %+v", joinNode, err)
				goto joinOp
			}

			// 已经是assign状态，继续执行下面的可重入代码即可
		}

		Logger.Printf("Successfully join assign, instance %s", c.instanceId)

		return nil

	default:
		return errors.Errorf("Unknown state %s", curState)
	}
}

// 等待value变为某个值
func (c *Coordinator) waitState(ctx context.Context, target string) error {

tryGetState:
	resp, err := c.etcdWrapper.get(ctx, c.etcdWrapper.nodeRbState(), nil)
	if err != nil {
		Logger.Printf("FAILED to get state, err %+v", err)

		select {
		case <-ctx.Done():
			Logger.Printf("waitState exit when %s", target)
			return errors.Errorf("unexpected exit when wait %s", target)
		default:
		}

		goto tryGetState
	}

	if resp.Count > 0 {
		v := stateValue(resp.Kvs[0].Value)
		state, leaseID := v.StateAndLeaseID()
		// state和leaseID要完全一致，防止instance在旧的rb中
		if leaseID == c.curG.Id {
			if state == target {
				Logger.Printf("State change to %s, return ASAP", target)
				return nil
			}
		} else {
			// rb执行到这里等待revoke，leader发起新rb时如果没有当前instance，state会走到revoke状态，这时当前instance应该尽快识别相应rb event，在handleRbEvent中revoke掉所有instance
			return errors.Errorf("FAILED to wait %s, because state's leaseID %d is difference from cur g's id %d", target, leaseID, c.curG.Id)
		}
	}

	node := c.etcdWrapper.nodeRbState()
tryWatch:
	wch := c.etcdWrapper.etcdClientV3.Watch(ctx, node, clientv3.WithRev(resp.Header.Revision), clientv3.WithFilterDelete())
	for {
		select {
		case <-ctx.Done():
			Logger.Printf("Instance %s goroutine exit when wait state %s", c.instanceId, target)
			return errors.Wrap(errClose, "")
		case wr := <-wch:
			if err := wr.Err(); err != nil {
				Logger.Printf("FAILED to waitState, err: %+v", err)
				goto tryWatch
			}

			for _, ev := range wr.Events {
				v := stateValue(ev.Kv.Value)
				state, _ := v.StateAndLeaseID()
				if state == target {
					Logger.Printf("Got waitState ev [%s]", ev.Kv.String())
					return nil
				}
			}
		}
	}
}

func (c *Coordinator) waitInstanceState(ctx context.Context, state string, participant []string) error {
	// 收集所有instance的join revoke请求，进入revoke状态

	// 收集current以及现存instance的assignment，新assign存入revoke

	firstTry := true
wait:
	select {
	case <-ctx.Done():
		Logger.Printf("Instance %s waitInstanceState exit when wait %s", c.instanceId, state)
		return errors.Wrap(errClose, "")
	default:
		if !firstTry {
			time.Sleep(defaultOpWaitTimeout)
		}
	}
	firstTry = false

	// return ASAP，上面判断是否需要trigger rb
	if time.Now().Unix()-c.newG.Timestamp >= defaultRbTimeout {
		return errors.Errorf("Rb timeout, start new g, cur g %+v", *c.newG)
	}

	instanceIdAndJoin, err := c.etcdWrapper.getRbInstanceIdAndJoin(ctx)
	if err != nil {
		Logger.Printf("FAILED to g join node, err %+v", err)
		goto wait
	}
	if len(instanceIdAndJoin) == 0 {
		Logger.Printf("Waiting instance join current g %+v on state %s", *c.newG, state)
		goto wait
	}

	var (
		instanceIds          []string
		foundUncompletedJoin bool
	)
	for instanceId, join := range instanceIdAndJoin {
		if join != state {
			// 有instance没有发送join revoke就continue，继续等待
			// worker一般直接创建revoke节点，所以数量对基本就ok，保险防止出现state异常的情况
			Logger.Printf("Waiting instance %s send join %s request, current state %s", c.instanceId, state, join)
			foundUncompletedJoin = true
		}
		instanceIds = append(instanceIds, instanceId)
	}
	if foundUncompletedJoin {
		goto wait
	}

	sort.Strings(instanceIds)
	sort.Strings(participant)

	if !reflect.DeepEqual(instanceIds, participant) {
		Logger.Printf(
			"Value not equal, new g(%d) joined instanceId %+v, participant %+v",
			c.newG.Id,
			instanceIds,
			participant,
		)
		goto wait
	}
	return nil
}

func (c *Coordinator) waitCompareAndSwap(ctx context.Context, g *G, node string, curValue string, newValue string) error {
	firstTry := true
tryCompareAndSwap:
	select {
	case <-ctx.Done():
		Logger.Printf(
			"Instance %s waitCompareAndSwap exit, when change %s from %s to %s",
			c.instanceId,
			node,
			curValue,
			newValue)
		return errors.Wrap(errClose, "")
	default:
		if !firstTry {
			time.Sleep(defaultOpWaitTimeout)
		}
	}
	firstTry = false

	// return ASAP，上面判断是否需要trigger rb
	if time.Now().Unix()-g.Timestamp >= defaultRbTimeout {
		return errors.Errorf("Rb timeout, g %+v", *g)
	}

	if err := c.etcdWrapper.compareAndSwap(ctx, node, curValue, newValue, -1); err != nil {
		if err == errNodeValueErr {
			return errors.Wrapf(err, "FAILED to compareAndSwap %s from %s to %s", node, curValue, newValue)
		}

		if err != errNodeValueAlreadyExist {
			// unexpected err
			Logger.Printf("FAILED to compareAndSwap %s from %s to %s, err: %+v", node, curValue, newValue, err)

			time.Sleep(defaultOpWaitTimeout)
			goto tryCompareAndSwap
		}

		// value是revoke，继续执行
	}
	return nil
}

func (c *Coordinator) waitAdjustAssignment(ctx context.Context, assignment string, fn func(ctx context.Context, assignment string) error) {
	firstTry := true

tryAdjust:
	if !firstTry {
		firstTry = false
		time.Sleep(defaultOpWaitTimeout)
	}

	select {
	case <-ctx.Done():
		Logger.Printf("waitAdjustAssignment exit, %s", assignment)
		return
	default:
	}

	if err := fn(ctx, assignment); err != nil {
		Logger.Printf("FAILED to adjust assignment, err %+v", err)
		goto tryAdjust
	}
}

func (c *Coordinator) assign(ctx context.Context, assignment string) error {
	// revoke失败，等待直到rb超时，goroutine退出，也可以直接返回，等待下次rb
	// 增加goto，可以引入retry功能
	assignedTasks, err := c.taskHub.OnAssigned(ctx, assignment)
	if err != nil {
		return errors.Wrapf(err, "FAILED to assign %s, because err %s", assignment, err)
	}
	if len(assignedTasks) == 0 {
		Logger.Print("No assigned tasks")
		return nil
	}
	Logger.Printf("Instance %s successfully assign tasks, finish OnAssigned", c.instanceId)

	for _, task := range assignedTasks {
		node := c.etcdWrapper.nodeTaskId(task.Key(ctx))

		// 上面的OnAssigned成功，表示任务已经分配下去，这里保证etcd中新增task节点。
	occupyTask:
		// 这里用leaseID，保证instance down掉的场景，自己的任务etcd节点也会被清除掉，减少下面occupyTask冲突的概率。
		taskOwnerInstance, err := c.etcdWrapper.createAndGet(ctx, node, c.instanceId, c.instanceLeaseID)
		if err == nil {
			Logger.Printf("Successfully bind %s to instance %s", node, c.instanceId)
			continue
		}

		if err != errNodeExist {
			Logger.Printf("FAILED to createAndGet node %s, err %+v", node, err)
			goto occupyTask
		}

		if taskOwnerInstance != c.instanceId {
			if err := c.canIgnoreInstance(ctx, taskOwnerInstance); err != nil {
				Logger.Printf("Unexpected err, node occupied by %s, %+v", taskOwnerInstance, err)
				// 仍旧强行删除只是打印err，用于追查问题
			}
			if err := c.etcdWrapper.del(ctx, node); err != nil {
				return errors.Wrap(err, "")
			}
			goto occupyTask
		}
	}
	Logger.Printf("Instance %s successfully assign tasks, finish etcd ops", c.instanceId)
	return nil
}

func (c *Coordinator) canIgnoreInstance(ctx context.Context, instanceId string) error {
	hbInstanceIds, err := c.etcdWrapper.getInstanceIds(ctx)
	if err != nil {
		return errors.Wrap(err, "")
	}

	// 没有hb
	var exist bool
	for _, id := range hbInstanceIds {
		if id == instanceId {
			exist = true
			break
		}
	}
	if !exist {
		Logger.Printf("Instance %s can be ignored because no hb", instanceId)
		return nil
	}

	// 在本次participant中，已经进入assign阶段，肯定revoke成功
	// 刚启动的时候走staticMembership，curG还没有初始化
	if c.curG != nil {
		for _, id := range c.curG.Participant {
			if id == instanceId {
				Logger.Printf("Instance %s can be ignored because in participant", instanceId)
				return nil
			}
		}
	}

	// 有hb，且不在participant中，等待watchHb触发rb

	return errors.Errorf("Can not ignore %s", instanceId)
}

func (c *Coordinator) revoke(ctx context.Context, assignment string) error {
	// revoke失败，等待直到rb超时，goroutine退出，也可以直接返回，等待下次rb
	// 增加goto，可以引入retry功能
	revokedTasks, err := c.taskHub.OnRevoked(ctx, assignment)
	if err != nil {
		return errors.Wrapf(err, "FAILED to revoke %s, because err %s", assignment, err)
	}

	if len(revokedTasks) == 0 {
		Logger.Print("No revoked tasks")
		return nil
	}

	// revoke成功，删除掉任务节点（用于防止并行任务）
	for _, task := range revokedTasks {
		node := c.etcdWrapper.nodeTaskId(task.Key(ctx))
		err := c.etcdWrapper.del(ctx, node)
		if err != nil {
			return errors.Wrapf(err, "FAILED to del node %s, err %+v", node, err)
		}

		Logger.Printf("Successfully revoke %s on instance %s", task.Key(ctx), c.instanceId)
	}
	Logger.Print("Successfully revoke all tasks")
	return nil
}

/*
leader监视heartbeat节点，负责触发rb

问题：如果若干event发生在leader重启期间，那么这些worker资源的变动存在很长时间不被发现的可能性，leader的存活虽然有多节点保证，但重启正好在上次hb为过期期间，那么不会有rb事件发生
注意：leader是否可能因为自身原因漏掉事件（节点的增加/删除），leader失效的场景下，如果有instance的增加/删除，都是观测不到的，leader在有意识重启的场景下无差别rb，保证当前正确，且之后的事件都能接收到
失效：leader所处进程遗漏事件的场景，无论人工重启还是异常原因（机器重启、网络异常），hb是宏观上确认进程存活的方法，可以放弃hb周期级别的误差（leader在发送hb之后理解down掉）

1 leader rolling bounce，不在的期间，一个instance下线，leader rejoin，需要触发rb
2 leader down，timeout后触发rb，竞争leader，触发rb

leader down（rolling bounce 或者 机器异常 或者 bug）发生在以下场景：
1 stable，等待leader被探测到down后，选举leader，新leader会触发rb
2 rebalancing，当前rb的follower join/sync group超时，选举leader 触发rb，每个instance的rb都是但goroutine在处理

watchHb的职责：
1 follower，no op
2 leader，rb，rb失败直到重启

新instance加入或者当前某个instance被移除，可以触发rb，如果正在进行rb，不在本次generation内的新instance，rmf应该拒绝加入（join group）请求，并开始重试，新instance本地没有正在运行的task，重试即可

旧instance hb发出不稳定导致被移除，但本地task还在执行，移除事件（instance不知道自己被etcd移除，所以还在干活）发生在：
rb获取instance snapshot之前，leader不能给该instance分任务，相同task对于并行敏感的情况，会出现冲突类错误，instance加入后重新触发rb恢复 FIXME
rb获取instance snapshot之后，leader分配任务给该节点，但长时间收不到sync group的response，rb timeout重新触发rb
*/
func (c *Coordinator) watchInstances(ctx context.Context, rev int64, stopper <-chan struct{}) error {
	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithPrefix())
	opts = append(opts, clientv3.WithRev(rev))

tryWatch:
	wch := c.etcdWrapper.etcdClientV3.Watch(ctx, c.etcdWrapper.nodeRbLeader(), opts...)
	for {
		select {
		case <-ctx.Done():
			Logger.Printf("watchInstances exit")
			return nil
		case <-stopper:
			Logger.Printf("Leader session done")
			return nil
		case wr := <-wch:
			if err := wr.Err(); err != nil {
				Logger.Printf("FAILED to watchInstances, err: %+v", err)

				if wr.Err().Error() == "etcdserver: mvcc: required revision has been compacted" {
					rev++
				}
				goto tryWatch
			}

			var rb bool
			for _, ev := range wr.Events {
				rev = ev.Kv.ModRevision + 1
				if ev.IsCreate() || ev.Type == clientv3.EventTypeDelete {
					Logger.Printf("Got hb rb ev: [%s]", ev.Kv.String())
					rb = true
					break
				}
			}

			if rb {
				if err := c.tryTriggerRb(ctx); err != nil {
					return errors.Wrap(err, "FAILED to tryTriggerRb")
				}

				if err := c.leaderHandleRb(ctx); err != nil {
					return errors.Wrap(err, "FAILED to handle new instance event from heartbeat")
				}
				Logger.Printf("leader handle rb completed g [%s]", c.newG.String())
			}
		}
	}
}

func (c *Coordinator) tryTriggerRb(ctx context.Context) error {
	locker, leaseID, err := c.etcdWrapper.acquireLock(ctx, defaultSessionTimeout)
	if err != nil {
		return errors.Wrap(err, "FAILED to acquire lock")
	}
	defer locker.Unlock()

tryTrigger:
	select {
	case <-ctx.Done():
		Logger.Printf("tryTriggerRb exit")
		return errors.New("tryTriggerRb exit")
	default:
	}

	canRetry, err := c.triggerRb(ctx, leaseID)
	if err != nil {
		Logger.Printf("FAILED to trigger rb canRetry %t, err %+v", canRetry, err)
	}
	if canRetry {
		time.Sleep(defaultOpWaitTimeout)
		goto tryTrigger
	}
	return err
}

func (c *Coordinator) triggerRb(ctx context.Context, leaseID clientv3.LeaseID) (bool, error) {
	// 生成新的g，利用leaseID保证互斥，防止split brain场景下，对于g的更新冲突
	newG := G{
		Timestamp: time.Now().Unix(),
		Id:        int64(leaseID),
	}

	// 提前设定参与本次rb的instance，如果依赖leader请求得到的snapshot，
	// 会增加因为新增instance导致rb失败的几率（新增instance在新g产生之后watch g，没有收到rb通知）
	// 发起会议，当前见到的instance都来，不来就再次rb，降低理解的难度
	hbInstanceIds, err := c.etcdWrapper.getInstanceIds(ctx)
	if err != nil {
		return true, errors.Wrap(err, "Failed to get active from node")
	}
	if len(hbInstanceIds) == 0 {
		Logger.Print("No hb instance exist")
		// 重试，但是不报错
		return true, nil
	}
	newG.Participant = append(newG.Participant, hbInstanceIds...)

	/*
		retry逻辑交给上层处理：
		1 重试 or 放弃，识别不出来的错误，例如etcd报错
		2 切换follower（返回false，就直接切换角色）

		defaultLeaderLockSessionTimeout的设定需要能够覆盖acquireLock成功之后的超时时间之和，减少因为操作耗时长引入的多leader并行trigger问题
		当前保证：
		1 lock -> 多个leader在defaultStateSessionTimeout内不能互斥访问trigger rb的逻辑（包括：state和g/id节点的更改）
		2 获取lock之后stop the world发生，其他leader拿到lock，在leaseID合法的情况下，允许开新g，前提是正在进行真实的rb动作才能取消当前g，相应新g
		3 访问etcd的逻辑都有超时保证，理论上不会block在triggerRb方法
	*/

	stateNode := c.etcdWrapper.nodeRbState()
	gidNode := c.etcdWrapper.nodeGId()

	stateGetOp := clientv3.OpGet(stateNode)
	gidGetOp := clientv3.OpGet(gidNode)
	getResp, err := c.etcdWrapper.etcdClientV3.Txn(context.TODO()).Then(stateGetOp, gidGetOp).Commit()
	if err != nil {
		return true, errors.Wrap(err, "")
	}
	if !getResp.Succeeded {
		return false, errors.Errorf("FAILED to get node %s, response: %s", stateNode, getResp.Responses[0].String())
	}

	newStateValue := formatStateValue(StateIdle.String(), leaseID)

	statePutOp := clientv3.OpPut(stateNode, newStateValue)
	gidPutOp := clientv3.OpPut(gidNode, newG.String())

	/*
		单纯的lock api不能防止由于STW导致的lock过期，但当前goroutine仍旧继续更新state的问题（state需要互斥访问）
		https://github.com/etcd-io/etcd/blob/master/Documentation/learning/why.md
		https://github.com/etcd-io/etcd/blob/master/Documentation/learning/lock/README.md
		需要通过compare and swap，预言当前state的当前值，创建或者swap失败认为有split brain情况发生
	*/

	if getResp.Responses[0].GetResponseRange().Count == 0 && getResp.Responses[1].GetResponseRange().Count == 0 {
		stateCmp := clientv3.Compare(clientv3.CreateRevision(stateNode), "=", 0)
		gidCmp := clientv3.Compare(clientv3.CreateRevision(gidNode), "=", 0)

		addResp, err := c.etcdWrapper.etcdClientV3.Txn(context.TODO()).If(stateCmp, gidCmp).Then(statePutOp, gidPutOp).Commit()
		if err != nil {
			return true, errors.Wrap(err, "")
		}
		if !addResp.Succeeded {
			return false, errors.Errorf("FAILED to add %s and %s, response1: %s, response2: %s", stateNode, gidNode, addResp.Responses[0].String(), addResp.Responses[1].String())
		}
	} else if getResp.Responses[0].GetResponseRange().Count == 1 && getResp.Responses[1].GetResponseRange().Count == 1 {
		curStateValue := string(getResp.Responses[0].GetResponseRange().Kvs[0].Value)

		// 只打印error，直接修正
		curState, curLeaseID := stateValue(curStateValue).StateAndLeaseID()
		if curState != StateIdle.String() {
			Logger.Printf("Found illegal state value when try to trigger rb, %s", curStateValue)
		}

		curGIdValue := string(getResp.Responses[1].GetResponseRange().Kvs[0].Value)
		g := ParseG(ctx, curGIdValue)
		if curLeaseID != g.Id {
			return false, errors.Errorf("Found inconsistent leaseID, state: %s, g: %s", curStateValue, curGIdValue)
		}

		stateCmp := clientv3.Compare(clientv3.Value(stateNode), "=", curStateValue)
		gidCmp := clientv3.Compare(clientv3.Value(gidNode), "=", curGIdValue)

		putResp, err := c.etcdWrapper.etcdClientV3.Txn(context.TODO()).If(stateCmp, gidCmp).Then(statePutOp, gidPutOp).Commit()
		if err != nil {
			return true, errors.Wrap(err, "")
		}
		if !putResp.Succeeded {
			return false, errors.Errorf("FAILED to put %s and %s, response1: %s, response2: %s", stateNode, gidNode, putResp.Responses[0].String(), putResp.Responses[1].String())
		}
	} else {
		return false, errors.Errorf("Found inconsistent state and g id, response1: %s, response2: %s", getResp.Responses[0].String(), getResp.Responses[1].String())
	}

	Logger.Printf("Successfully trigger rb on new g [%s]", newG.String())

	// 清理现有g的数据
	c.tryCleanExpiredGDataNode(ctx)

	c.newG = &newG
	return false, nil
}

func (c *Coordinator) tryCleanExpiredGDataNode(ctx context.Context) {
	if err := c.etcdWrapper.del(ctx, c.etcdWrapper.nodeGJoin()); err != nil {
		Logger.Printf("FAILED to del g join, err: %+v", err)
	}
	if err := c.etcdWrapper.del(ctx, c.etcdWrapper.nodeGAssign()); err != nil {
		Logger.Printf("FAILED to del g assign, err: %+v", err)
	}
}
