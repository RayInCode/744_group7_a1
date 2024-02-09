#!/bin/bash
hdfs dfs -mkdir /data  #create a direct to store data and result in hdfs
hdfs dfs -put /proj/uwmadison744-s24-PG0/data-part3/enwiki-pages-articles/ /data
spark-submit --master spark://10.10.1.1:7077 task4.py

# trigger to kill a woker on node0 acoording to the dynamic message like [Stage 8:===============================>      (116 + 15) / 200]
# to simulate kill a worker at the 25% of the application's lifetime
# sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
# kill [process_num]