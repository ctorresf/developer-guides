#!/usr/bin/bash 

python -m src.generator.generator --generator_type helicopter_teams \
--race_id race_01 \
--num_teams 10 \
--output file_path \
--output_path output/landing_zone/cup/teams/race_01_teams-000-of-001.json


## Create race events
echo "Creating Race Events"
python -m src.generator.generator --generator_type race_event \
--race_id race_01 \
--num_teams 10 \
--num_records 100 \
--output file_path \
--output_path output/landing_zone/cup/events/race_01_events-000-of-001.json


python -m src.generator.generator --generator_type race_event \
--race_id race_02 \
--num_teams 10 \
--num_records 100 \
--output file_path \
--output_path output/landing_zone/cup/events/race_02_events-000-of-001.json


python -m src.generator.generator --generator_type race_event \
--race_id race_03 \
--num_teams 10 \
--num_records 100 \
--output file_path \
--output_path output/landing_zone/cup/events/race_03_events-000-of-001.json


## Create telemetry data
echo "Creating Telemetry Data"
python -m src.generator.generator --generator_type telemetry \
--race_id none \
--num_teams 10 \
--num_records 1000 \
--output file_path \
--output_path output/landing_zone/cup/telemetry/race_none_telemetry-000-of-001.json

python -m src.generator.generator --generator_type telemetry \
--race_id race_01 \
--num_teams 10 \
--num_records 1000 \
--output file_path \
--output_path output/landing_zone/cup/telemetry/race_01_telemetry-001-of-002.json

python -m src.generator.generator --generator_type telemetry \
--race_id race_02 \
--num_teams 10 \
--num_records 1000 \
--output file_path \
--output_path output/landing_zone/cup/telemetry/race_02_telemetry-001-of-002.json


## Create fan engagement data
echo "Creating Fan Engagement Data"
python -m src.generator.generator --generator_type fan_engagement \
--race_id league:04 \
--num_teams 10 \
--num_records 100 \
--output file_path \
--output_path output/landing_zone/cup/fan_engagement/league04_fan_engagement-000-of-001.json

python -m src.generator.generator --generator_type fan_engagement \
--race_id race_11 \
--num_teams 10 \
--num_records 100 \
--output file_path \
--output_path output/landing_zone/cup/fan_engagement/race11_fan_engagement-000-of-001.json

python -m src.generator.generator --generator_type fan_engagement \
--race_id 'Cup 25' \
--num_teams 10 \
--num_records 100 \
--output file_path \
--output_path output/landing_zone/cup/fan_engagement/cup25_fan_engagement-000-of-001.json
