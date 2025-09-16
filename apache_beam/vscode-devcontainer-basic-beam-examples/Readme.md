# Apache Beam Basic Examples in VSCode DevContainer

This module contains practical examples for data processing using [Apache Beam](https://beam.apache.org/) in a VSCode DevContainer environment.

## Project Structure

- **.devcontainer/**
  - [`devcontainer.json`](.devcontainer/devcontainer.json): DevContainer configuration for VSCode, including Python setup and recommended extensions.
- **data/**
  - [`pg84.txt`](data/pg84.txt): Sample text data (Frankenstein from Project Gutenberg) for word count and other processing examples.
- **scripts/**
  - [`install-dependencies.sh`](scripts/install-dependencies.sh): Bash script to install Python dependencies and tools inside the container.
- **src/**
  - [`cleaning_data.py`](src/cleaning_data.py): Example pipeline for cleaning and standardizing tabular data.
  - [`join.py`](src/join.py): Demonstrates joining two datasets (customers and orders) using Beam.
  - [`json_to_formats.py`](src/json_to_formats.py): Converts JSON data to Avro and Parquet formats using Beam.
  - [`side_input.py`](src/side_input.py): Shows how to use side inputs to enrich main data with lookup tables.
  - [`simple_pipeline.py`](src/simple_pipeline.py): Basic pipeline for cleaning and formatting names.
  - [`word_count_map.py`](src/word_count_map.py): Word count example using Map and combiners.
  - [`word_count_pardo.py`](src/word_count_pardo.py): Word count using ParDo for splitting and formatting.
  - [`word_count_ptransform.py`](src/word_count_ptransform.py): Word count using a custom PTransform for encapsulating logic.
- [`requirements.txt`](requirements.txt): Python dependencies for running all examples.

## How to Use

1. **Open in VSCode**  
   Open the folder in VSCode and select "Reopen in Container" to start the DevContainer.

2. **Dependencies**  
   The container will automatically run [`scripts/install-dependencies.sh`](scripts/install-dependencies.sh) to install all required packages.

3. **Run Examples**  
   Use the following commands to run the examples:

   ```sh
   python src/simple_pipeline.py --runner DirectRunner --output_folder output --output_prefix sample0
   python src/word_count_map.py --runner DirectRunner --input data/pg84.txt --output output/pg84_word_count_with_map
   ```

   Create the Dag representation in a png file

   ```
    python src/simple_pipeline.py  --output_folder output --output_prefix sample0 --runner apache_beam.runners.render.RenderRunner --render_output dag.png 
   ```

   Create a interactive Dag

   ```
    python src/simple_pipeline.py --output_folder output --output_prefix sample0  --runner apache_beam.runners.render.RenderRunner --render_port 8089
   ```

   See each script for more usage details.

4. **View Results**  
   Output files are saved in the `output/` folder. Use the recommended VSCode extensions (Avro Viewer, Parquet Viewer, etc.) for visualization.

## Requirements

- Docker
- Visual Studio Code with DevContainers support

---

For more details, see the comments in each source file in.
