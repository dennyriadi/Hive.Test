CREATE TABLE event(
    ID int,
    name string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:hadoop.tmp.dir}/customSerde';
