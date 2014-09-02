Python Rendler Framework
========

```bash
$ vagrant ssh
vagrant@mesos:~ $ cd sandbox/python
# Start the scheduler with the seed url, the mesos master ip and optionally a task limit 
vagrant@mesos:python $ python rendler.py http://mesosphere.io 127.0.1.1:5050 42
# <Ctrl+C> to stop...
```
