import argparse
import apache_beam as beam
import os

# This is the function that will be applied to each element in the PCollection.
def clean_and_format_name(name):
    """
    Cleans a name (removes whitespace, capitalizes) and formats it.
    """
    cleaned_name = name.strip().title()
    length = len(cleaned_name)
    return f"Name: {cleaned_name}, Length: {length}"

def run_pipeline(argv=None):
    """
    Main function to run the Beam pipeline.
    """
    # 1. Create a custom argument parser and get the Beam options.
    #    This is the standard way to handle command-line parameters in Beam.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_folder',
        dest='output_folder',
        required=True,
        help='Output file path to write results to.')
    parser.add_argument(
        '--output_prefix',
        dest='output_prefix',
        required=True,
        help='Output file prefix.')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Pass the parsed arguments to the Beam pipeline.
    # Beam will automatically handle its own arguments (like --runner).
    options = beam.options.pipeline_options.PipelineOptions(pipeline_args)
    
    # 2. Create the Beam pipeline object.
    with beam.Pipeline(options=options) as pipeline:
        
        # 3. Define the Input (Extract) - Create the PCollection.
        #    We use beam.Create() to create a PCollection from an in-memory list.
        raw_names = (
            pipeline
            | 'Create Raw Names' >> beam.Create([
                '  alice  ',
                '  bOb  ',
                'Charlie ',
                'David '
            ])
        )
        
        # 4. Define the Transformation (Transform).
        #    We use 'Map' to apply our function to each name.
        cleaned_names = (
            raw_names
            | 'Clean and Format' >> beam.Map(clean_and_format_name)
        )
        
        # 5. Define the Output (Load).
        full_path = os.path.join(known_args.output_folder, known_args.output_prefix)
        cleaned_names | 'Write to File' >> beam.io.WriteToText(full_path, file_name_suffix='.txt')


if __name__ == '__main__':
    run_pipeline()