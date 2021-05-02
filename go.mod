module github.com/entertainment-venue/lrmf

go 1.15

require (
	github.com/coreos/etcd v3.3.25+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e
	github.com/golang/protobuf v1.4.3 // indirect
	github.com/google/uuid v1.2.0 // indirect
	github.com/pkg/errors v0.9.1
	go.uber.org/zap v1.16.0 // indirect
	google.golang.org/genproto v0.0.0-20210211221406-4ccc9a5e4183 // indirect
	google.golang.org/grpc v1.35.0 // indirect
)

replace github.com/coreos/go-systemd => github.com/coreos/go-systemd/v22 v22.1.0

// https://blog.csdn.net/qq_43442524/article/details/104997539
replace google.golang.org/grpc => google.golang.org/grpc v1.26.0
