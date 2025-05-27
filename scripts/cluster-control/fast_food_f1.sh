#!/bin/bash

case $1 in
"start")
        echo " --------启动 hadoop104 业务数据flume-------"
        ssh hadoop104 "nohup /opt/module/flume/bin/flume-ng agent -n a1 -c /opt/module/flume/conf -f /opt/module/flume/job/fast_food_kafka_to_hdfs_db.conf >>/opt/module/flume/fast_food_f1.sh 2>&1 &"
;;
"stop")
        echo " --------停止 hadoop104 业务数据flume-------"
        ssh hadoop104 "ps -ef | grep fast_food_kafka_to_hdfs_db | grep -v grep |awk '{print \$2}' | xargs -n1 kill"
;;
esac
