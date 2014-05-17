#! /bin/bash

tar -czf ../laughing-adventure.tgz .
s3cmd put ../laughing-adventure.tgz s3://downloads.mesosphere.io/demo/
