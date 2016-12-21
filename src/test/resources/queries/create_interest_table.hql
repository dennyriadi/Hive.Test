CREATE EXTERNAL TABLE interest (
	profileId STRING,
    interestId INT,
	title STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION '${hiveconf:hadoop.tmp.dir}/interest';

MSCK REPAIR TABLE interest;
