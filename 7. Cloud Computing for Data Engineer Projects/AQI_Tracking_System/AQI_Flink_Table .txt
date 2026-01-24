%flink.ssql
CREATE TABLE air_quality_with_watermark_1 (
  -- Core data fields
  `location_id` STRING,
  `location_name` STRING,
  `parameter` STRING,
  `value` DOUBLE,
  `unit` STRING,
  
  -- Event time field (using metadata timestamp as fallback)
  `processing_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  
  -- Other non-temporal fields
  `timezone` STRING,
  `latitude` DOUBLE,
  `longitude` DOUBLE,
  `country_iso` STRING,
  `isMobile` BOOLEAN,
  `isMonitor` BOOLEAN,
  `owner_name` STRING,
  `provider` STRING,
  
  -- Metadata fields
  `shard_id` STRING METADATA FROM 'shard-id',
  `sequence_number` STRING METADATA FROM 'sequence-number',
  
  -- Watermark using processing time
  WATERMARK FOR `processing_time` AS `processing_time` - INTERVAL '5' SECOND
)
WITH (
  'connector' = 'kinesis',
  'stream' = 'AQI_Stream',
  'aws.region' = 'us-east-1',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'scan.stream.initpos' = 'TRIM_HORIZON'
);

