#!/usr/bin/env bash
# 启动flink job
cd `dirname $0`

command=$1

if  [ ! $command ];then
  command="start"
fi

FLINK_DIR=/usr/local/flink-1.10.1
PROJECT_DIR=`pwd`

function kill_app(){

	appId=`yarn application -list |grep mysql-binglog-write-hdfs|gawk '{print $1}'`
	if  [  ${appId} ];then
     		yarn application -kill $appId
     		echo "kill exist appid $appId"
	fi
}

function start_app_temp(){
	${FLINK_DIR}/bin/flink run -d  -m yarn-cluster -yn 16 -ys 1 -p 16  -yjm 1024 -ytm 1024 -ynm log-landing-raw -c com.pactera.ExcuteObject \
	/srv/app/log-landing/new-energy-msg-0.0.1-SNAPSHOT.jar \
	--runMode pro \
	--pp 16 \
	--ep 16 \
	--brokers 192.168.147.178:9092,192.168.147.179:9092,192.168.147.180:9092 \
	--groupid test_ecpt_yarn_001 \
	--sourceTopic base_data_input_16 \
	--offsetreset smallest \
	--checkpointUri "hdfs://192.168.147.170:9000/tmp/flink/" \
	--schemaName eye \
	--tableName raw\
	--url jdbc:clickhouse://192.168.145.63:9090 \
	--user insert \
	--password ""  \
	--batchSize 20000 \
	--batchInterval 10000 \
	--hadoopUserName hadoop

}

function start_app_pro(){
	${FLINK_DIR}/bin/flink run -d  -m yarn-cluster -ys 2 -p 8  -yjm 1024 -ytm 2048 -ynm mysql-binglog-write-hdfs \
	-c com.github.guozhen.Kafka2Hdfs \
	./flink-binglog-write-hadoop.jar \
	--file.Rollover.interval 60 \
	--hdfs.output.path "hdfs://namenode1:8020//user/flink110/test1/flink-data-gz/" \
	--flink.fs.default-scheme "hdfs://namenode1:8020" \
	--flink.checkpoint.uri "hdfs:///user/flink110/flink-checkpoints-gz/" \
	--flink.checkpoint.interval 30000 \
	--flink.checkpoint.min.interval 500 \
	--flink.partition.parallelism 1 \
	--kafka.bootstrap.servers "kafka1:9092,kafka2:9092,kafka3:9092" \
	--kafka.topics "test" \
	--kafka.group.id "gz_test_01"\
	--kafka.auto.offset.reset "latest" \
	--kafka.enable.auto.commit "true" \
	--kafka.auto.commit.interval.ms 5000  \
	--mysql.url "jdbc:mysql://10.19.1.29:3306/temp?useUnicode=true&characte&useSSL=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false" \
	--mysql.username "zxber" \
	--mysql.password "8in3PtpeO1dRvxzd7vzTqlSxxxzOr1AG" \
	--hadoop.user.name "hadoop"

}

if [ ${command} = "start" ];then
	kill_app
	start_app_pro
elif [ ${command} = "stop" ];then
	kill_app
fi


