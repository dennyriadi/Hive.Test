CREATE EXTERNAL TABLE profile (
    id STRING,
    type STRING,
    created TIMESTAMP,
    score INT,
    active BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION '${hiveconf:hadoop.tmp.dir}/profile';

MSCK REPAIR TABLE profile;
