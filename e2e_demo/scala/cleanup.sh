#!/bin/bash

set -ex

rm -rf sparkdemoproject-3 spark-* *.jar *.tgz *.gz hadoop-*
rm -rf ../../pipelinecompare/warehouse/*
rm -f /tmp/spark-migration-jars/*