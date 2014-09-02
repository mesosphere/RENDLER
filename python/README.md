Python Rendler Framework
========

```bash
you@yourhost $ vagrant ssh
vagrant@mesos:~ $ cd ~/sandbox/mesosphere/mesos-sdk/RENDLER/python
# Start the scheduler with the seed url, the mesos master ip and optionally a task limit 
vagrant@mesos:python $ python rendler.py http://mesosphere.io 127.0.1.1:5050 42
# <Ctrl+C> to stop...
vagrant@mesos:~ $ mv result.dot ..
vagrant@mesos:~ $ ../bin/make-pdf
you@yourhost $ open ../result.pdf
```
