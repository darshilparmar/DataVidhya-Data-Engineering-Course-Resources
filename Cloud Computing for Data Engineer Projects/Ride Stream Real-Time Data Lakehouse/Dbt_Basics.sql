schema.yml


version: 2

sources:
  - name: taxi_source
    database: awsdatacatalog   # the catalog (always this in Athena)
    schema: aqi_db             # your Glue database
    tables:
      - name: facts            # your Glue table name
	  
	  

profiles.yml


facts_trip.sql model

with trips as (

    select
        vendor_key,
                payment_key,
                location_key,
                trip_date
    from {{ source('taxi_source', 'facts') }}

)

select * from trips


DBT Profile Connection 

Athena_Connect:
  outputs:
    dev:
      type: athena
      database: awsdatacatalog   # <-- always this
      schema: aqi_db             # <-- your Glue database
      region_name: us-east-1
      s3_data_dir: s3://aws-glue-assets/output/processed/
      s3_staging_dir: s3://aws-glue-assets/glue/
      threads: 1
  target: dev