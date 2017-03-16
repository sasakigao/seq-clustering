#!/bin/bash

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue default \
    --driver-memory 8G \
    --executor-memory 2G \
    --num-executors 50 \
    --class com.sasaki.train.ReducedClu \
    --conf spark.default.parallelism=100 \
/home/sasaki/netease/ver2/pack/dimred_2.10-1.0.jar \
"hdfs:///netease/temp" \
"bkm" \
6