#!/usr/bin/bash 

echo "Create a list of equipment in a Minio bucket"
python -m src.generator.generator \
    --generator_type helicopter_teams \
    --output minio \
    --num_teams 10 \
    --race_id cup_jun_2025 \
    --output_path cup_jun_2025/helicopter_teams/teams.json