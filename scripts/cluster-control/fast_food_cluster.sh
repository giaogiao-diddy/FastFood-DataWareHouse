#!/bin/bash

case $1 in
"start"){
    echo ================== 启动 集群 ==================

        #启动 Zookeeper集群
        zk.sh start

        #启动 Hadoop集群
        myhadoop.sh start

        #启动 Kafka采集集群
        kf.sh start

        #启动业务消费 Flume
        fast_food_f1.sh start

        #启动 maxwell
        mxw.sh start

        };;
"stop"){
    echo ================== 停止 集群 ==================

        #停止 Maxwell
        mxw.sh stop

        #停止业务消费Flume
        fast_food_f1.sh stop

        #停止 Kafka 集群
        kf.sh stop

        #停止 Hadoop 集群
        myhadoop.sh stop

        #循环直至 Kafka 集群进程全部停止
        kafka_count=$(xcall jps | grep Kafka | wc -l)
        while [ $kafka_count -gt 0 ]
        do
            sleep 1
            kafka_count=$(jpsall | grep Kafka | wc -l)
            echo "当前未停止的 Kafka 进程数为 $kafka_count"
        done

        #停止 Zookeeper集群
        zk.sh stop

};;
esac
