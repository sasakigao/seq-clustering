#!/bin/bash

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue default \
    --driver-memory 12G \
    --executor-memory 6G \
    --num-executors 50 \
    --executor-cores 8 \
    --class com.sasaki.reduction.Dimred \
    --conf spark.default.parallelism=100 \
/home/sasaki/netease/ver2/dimred/dimred_2.10-1.0.jar \
"hdfs:///netease/ver2/seq2vec/916main/2to40/k5/raw/unaligned" \
5 \
5