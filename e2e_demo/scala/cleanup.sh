#!/bin/bash

rm -rf sparkdemoproject-3 spark-* *.jar *.tgz *.gz hadoop-*
rm -rf ../../pipelinecompare/warehouse/*
cd sparkdemoproject && gradle clean