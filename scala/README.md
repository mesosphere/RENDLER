Scala Rendler Framework
========

The Scala rendler must be executed inside the "vagrant ssh" session.

```bash
$ cd hostfiles/scala
$ sbt "run http://mesosphere.com 127.0.1.1:5050"
## press ENTER to initiate a graceful shutdown
## ...
$ ../bin/make-pdf
```
