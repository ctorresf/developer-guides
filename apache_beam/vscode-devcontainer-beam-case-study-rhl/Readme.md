# RHL Data pipelines

## Create a Dag png file

```
python <beam_pipeline>.py --runner apache_beam.runners.render.RenderRunner --render_output dag.png
```

## Create a interactive Dag

```
python <beam_pipeline>.py --runner apache_beam.runners.render.RenderRunner --render_port 8089
```

## Run Data pipelines for local files

### Convert an in-memory dataset of telemetry data to Avro

python src/beam_datapipeline/convert_data_v0_telemetry.py \
 --input_path output/landing_zone/demo/demo_telemetry_data.json \
 --output_path output/golden_zone/demo/demo_telemetry_data \
 --output_format avro 

### Convert an in-memory dataset of telemetry data to Parquet

python src/beam_datapipeline/convert_data_v0_telemetry.py \
 --input_path output/landing_zone/demo/demo_telemetry_data.json \
 --output_path output/golden_zone/demo/demo_telemetry_data \
 --output_format parquet 

### For the nest pipelines, we need to create some mock data

sh scripts/create_mock_data_localfile.sh 

### Convert files of telemetry data to Avro

python src/beam_datapipeline/convert_data_v1_telemetry.py \
 --input_path output/landing_zone/cup/telemetry/*.json \
 --output_path output/golden_zone/converted_telemetry_data/telemetry \
 --output_format avro 

### Convert files of telemetry data to Parquet

python src/beam_datapipeline/convert_data_v1_telemetry.py \
 --input_path output/landing_zone/cup/telemetry/*.json \
 --output_path output/golden_zone/converted_telemetry_data/telemetry \
 --output_format parquet 


 ### Convert files to Avro

python src/beam_datapipeline/convert_data_v2_generic.py \
 --input_path output/landing_zone/cup/telemetry/*.json \
 --avro_schema_path schemas/HelicopterTelemetry_v1.avsc \
 --output_path output/golden_zone/converted_generic/telemetry \
 --output_format avro 

python src/beam_datapipeline/convert_data_v2_generic.py \
 --input_path output/landing_zone/cup/telemetry/*.json \
 --avro_schema_path schemas/HelicopterTelemetry_v2.avsc \
 --output_path output/golden_zone/converted_generic/telemetry_v2 \
 --output_format avro 

python src/beam_datapipeline/convert_data_v2_generic.py \
 --input_path output/landing_zone/cup/teams/*.json \
 --avro_schema_path schemas/Team.avsc \
 --output_path output/golden_zone/converted_generic/Team \
 --output_format parquet 


### Join Race events and Teams data

python src/beam_datapipeline/event_team_joiner.py \
 --input_events output/landing_zone/cup/events/*.json \
 --input_teams output/landing_zone/cup/teams/*.json\
 --output output/golden_zone/events_with_team_data/data 


### 
race_data_joiner_v1.py     


###
race_data_joiner_v2.py


## Run Data pipelines for Kafka topic 

### Consume race data, save it to a Minio bucket, and send it to the telemetry system
python -m src.beam_datapipeline_kafka.race_events_processor \
    --runner PrismRunner  \
    --output_bucket race-event-bucket  \
    --output_file_prefix cup_jun_2025/events/race_events  \
    --num_records 100 \
    --input_topic race_events_topic \
    --output_topic telemetry_events_topic

After run the beam pipeline, you need to create the mock data and send to kafka topic: 

python -m src.generator.generator \
    --generator_type race_event \
    --output kafka \
    --num_teams 10 \
    --race_id cup_jun_2025 \
    --num_records 100 \
    --output_path race_events_topic

### Create telemetry data and publish to Kafka
python -m src.generator.generator \
    --generator_type telemetry \
    --output kafka \
    --num_teams 10 \
    --race_id cup_jun_2025 \
    --num_records 100 \
    --output_path telemetry_topic

### Consume race event data and telemetry and save locally
python -m src.beam_datapipeline_kafka.telemetry_processor_file \
    --runner PrismRunner  \
    --output_bucket output  \
    --output_file_prefix cup_jun_2025/telemetry/  \
    --num_records 100 \
    --input_events_topic telemetry_events_topic \
    --input_telemetry_topic telemetry_topic 

### Consume race event data and telemetry and save to bucket
python -m src.beam_datapipeline_kafka.telemetry_processor_file \
    --runner PrismRunner  \
    --output_bucket s3://ai-bucket  \
    --output_file_prefix cup_jun_2025/telemetry/  \
    --num_records 100 \
    --input_events_topic telemetry_events_topic \
    --input_telemetry_topic telemetry_topic 

## Consumir datos de eventos de carrera y telemetria y guardar en redis y postgresql
python -m src.beam_datapipeline_kafka.telemetry_processor \
    --runner PrismRunner  \
    --output_bucket s3://ai-bucket  \
    --output_file_prefix cup_jun_2025/telemetry/  \
    --num_records 100 \
    --input_events_topic telemetry_events_topic \
    --input_telemetry_topic telemetry_topic \
    --output_table_name telemetry_data_table

