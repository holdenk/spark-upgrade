#!/bin/bash

rm -rf sparkdemoproject-3 spark-* *.jar *.tgz *.gz hadoop-*
rm -rf ../../pipelinecompare/warehouse/*
rm -f /tmp/spark-migration-jars/*
cd sparkdemoproject && gradle clean