ADD JAR src/test/resources/jars/json-serde.jar;

SET hive.support.sql11.reserved.keywords=false;

CREATE EXTERNAL TABLE email_event (
  sgAccountName STRING,
  orgId INT,
  event STRING,
  sgEventId STRING,
  sgMessageId STRING,
  emailSubject STRING,
  emailEvent STRING,
  emailSendTimestamp TIMESTAMP,
  emailSenderId STRING
)
PARTITIONED BY (date STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${hiveconf:hadoop.tmp.dir}/emailevents';

MSCK REPAIR TABLE email_event;
