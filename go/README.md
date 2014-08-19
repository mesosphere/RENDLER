Go Rendler Framework
========

Once you are ssh'd into the `mesos-demo` VM:

```bash
cd hostfiles/go
export GOPATH=$PWD
go get code.google.com/p/goprotobuf/{proto,protoc-gen-go}
go get github.com/mesosphere/mesos-go/mesos
go install github.com/mesosphere/rendler/scheduler
./bin/scheduler -seed http://mesosphere.io -master 127.0.1.1:5050 -local
```

### Generate graph

```bash
RENDLER_HOME=. ../bin/make-pdf
```

