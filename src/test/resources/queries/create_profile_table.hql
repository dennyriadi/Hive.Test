ADD JAR ${hiveconf:hadoop.jar.dir}/csv-serde.jar;

SET mapred.output.compress=true;
SET hive.exec.compress.output=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET hive.hadoop.supports.splittable.combineinputformat=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=500;

SET mapreduce.map.memory.mb=5120;
SET mapreduce.map.java.opts=-Xmx4608m;
SET mapreduce.reduce.memory.mb=5120;
SET mapreduce.reduce.java.opts=-Xmx4608m;

CREATE EXTERNAL TABLE profile (
    id STRING,
    type STRING,
    created TIMESTAMP,
    score INT,
    active BOOLEAN
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE
LOCATION '${hiveconf:hadoop.tmp.dir}/profile';

MSCK REPAIR TABLE profile;
