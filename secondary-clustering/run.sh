#!/bin/bash

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue default \
    --driver-memory 12G \
    --executor-memory 6G \
    --num-executors 50 \
    --class com.sasaki.loader.SecnCluster \
    --conf spark.default.parallelism=100 \
/home/sasaki/netease/ver2/secnclu/secnclu_2.10-1.0.jar \
"hdfs:///netease/ver2/seq2vec/916main/2to40/k5/raw/unaligned" \
3,6 \
km \
10