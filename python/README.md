Python Rendler Framework
========

```bash
$ vagrant ssh
vagrant@mesos:~ $ cd hostfiles
# Start the scheduler with the seed url, the mesos master ip and optionally a task limit 
vagrant@mesos:python $ python python/rendler.py http://mesos.apache.org 127.0.1.1:5050 10
# <Ctrl+C> to stop...
```
