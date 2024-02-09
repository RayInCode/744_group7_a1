#!/bin/bash
hdfs dfs -mkdir /data  #create a direct to store data and result in hdfs
hdfs dfs -put /proj/uwmadison744-s24-PG0/data-part3/enwiki-pages-articles/ /data
spark-submit --master spark://10.10.1.1:7077 task2.py