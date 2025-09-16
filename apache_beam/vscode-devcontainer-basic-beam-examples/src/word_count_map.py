import argparse
import re
import apache_beam as beam

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='input.txt',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args()

    # Create the pipeline object.
    with beam.Pipeline(argv=pipeline_args) as pipeline:

        # PCollection 1: Read the text file.
        lines = pipeline | 'ReadFromText' >> beam.io.ReadFromText(known_args.input)

        # PCollection 2: Split each line into individual words.
        words = (
            lines
            | 'SplitLinesIntoWords' >> beam.FlatMap(lambda line: re.findall(r'[a-zA-Z]+', line))
        )

        # PCollection 3: Count the occurrences of each word.
        word_counts = (
            words
            | 'CountWords' >> beam.combiners.Count.PerElement()
        )

        # PCollection 4: Format the output and write it to a file.
        formatted_output = (
            word_counts
            | 'FormatCounts' >> beam.Map(lambda word_count: f"{word_count[0]}: {word_count[1]}")
        )
        
        # Load the final results into a text file.
        formatted_output | 'WriteToText' >> beam.io.WriteToText(known_args.output, file_name_suffix='.txt')

if __name__ == '__main__':
    run()