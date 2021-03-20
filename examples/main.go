package main

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

func main() {
	testEtcdClientV3Campaign()
}

func testEtcdClientV3Campaign() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 3 * time.Second,
	})

	session, err := concurrency.NewSession(client, concurrency.WithTTL(10))
	if err != nil {
		panic(err)
	}

	election := concurrency.NewElection(session, "/testEtcdClientV3Campaign")
	if err := election.Campaign(context.TODO(), "foo"); err != nil {
		panic(err)
	}

	stopper := make(chan int)
	<-stopper
}
