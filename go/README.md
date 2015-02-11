Go Rendler Framework
========

Once you are ssh'd into the `mesos-demo` VM:

```bash
cd hostfiles/go
export GOPATH=$PWD
go get golang.org/x/net/context
go get code.google.com/p/go-uuid/uuid
go get github.com/golang/glog
go get github.com/gogo/protobuf/proto
go get github.com/stretchr/testify/mock
go get github.com/mesos/mesos-go/mesos
go install github.com/mesosphere/rendler/scheduler
./bin/scheduler -seed http://mesosphere.com -master 127.0.1.1:5050 -local
```

### Generate graph

```bash
RENDLER_HOME=. ../bin/make-pdf
```

