#!/usr/bin/bash 

echo "Create race data and publish to Kafka topic"
python -m src.generator.generator \
    --generator_type race_event \
    --output kafka \
    --num_teams 10 \
    --race_id cup_jun_2025 \
    --num_records 100 \
    --output_path race_events_topic