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

If you're using the VM for the first time, `sbt` will not be installed. Run the following commands to install it, as provided by the [sbt docs](http://www.scala-sbt.org/0.13/tutorial/Installing-sbt-on-Linux.html#Ubuntu+and+other+Debian-based+distributions):

```bash
$ echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
$ sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823
$ sudo apt-get update
$ sudo apt-get install sbt
```
