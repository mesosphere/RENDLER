Go Rendler Framework
========

The way too short build instructions:

```bash
export GOPATH=$PWD
go get code.google.com/p/goprotobuf/{proto,protoc-gen-go}
go get github.com/mesosphere/mesos-go/mesos
go install rendler
./bin/rendler -master <leading-master> -local
```

### Generate graph

```bash
RENDLER_HOME=. ../bin/make-pdf
```

