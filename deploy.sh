#! /bin/bash

tar -czf ../rendler.tgz .
s3cmd put ../rendler.tgz s3://downloads.mesosphere.io/demo/
