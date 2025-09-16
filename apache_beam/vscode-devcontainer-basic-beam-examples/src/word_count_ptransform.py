import argparse
import re
import apache_beam as beam

# Define a custom PTransform to encapsulate the word counting logic.
class CountWords(beam.PTransform):
    def expand(self, pcoll):
        # The 'expand' method defines the series of transformations.
        # It takes the input PCollection as an argument.
        
        # Step 1: Split each line into individual words.
        words = pcoll | 'SplitLinesIntoWords' >> beam.FlatMap(
            lambda line: re.findall(r'[a-zA-Z\']+', line)
        )

        # Step 2: Count the occurrences of each word.
        word_counts = words | 'CountWords' >> beam.combiners.Count.PerElement()
        
        # The expand method must return the final PCollection.
        return word_counts

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

        # Extract: Read the text file.
        lines = pipeline | 'ReadFromText' >> beam.io.ReadFromText(known_args.input)

        # Transform: Use our custom PTransform to perform the word count.
        # The pipeline now looks much cleaner and is easy to read.
        word_counts = lines | 'Count the words' >> CountWords()

        # Load: Format the output and write it to a file.
        formatted_output = (
            word_counts
            | 'Format for output' >> beam.Map(lambda word_count: f"{word_count[0]}: {word_count[1]}")
        )
        
        formatted_output | 'WriteToText' >> beam.io.WriteToText(known_args.output)

if __name__ == '__main__':
    run()