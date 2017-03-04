#!/bin/bash

spark-submit \
    --master local[4] \
    --deploy-mode client \
    --queue default \
    --driver-memory 512M \
    --executor-memory 512M \
    --num-executors 3 \
/home/sasaki/dev/gamease/route2/seq-clustering/target/scala-2.10/clustering_2.10-1.0.jar
