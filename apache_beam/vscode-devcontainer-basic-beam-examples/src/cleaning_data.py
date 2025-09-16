import apache_beam as beam

with beam.Pipeline() as pipeline:
    raw_data = pipeline | 'Load data' >> beam.Create([
        {'id': 1, 'name': 'Juan', 'age': 30},
        {'id': 2, 'name': None, 'age': 25},
        {'id': 3, 'name': 'Ana', 'age': None},
        {'id': 3, 'name': 'Ana', 'age': None}
    ])

    # Filter out rows where 'name' is null
    valid_data = (
        raw_data
        | 'Filter Nulls' >> beam.Filter(lambda row: row['name'] is not None)
    )

    # Set 'age' to 0 if it is null
    def clean_age(row):
        if row['age'] is None:
            row['age'] = 0
        return row

    clean_data = (
        valid_data
        | 'Set default values' >> beam.Map(clean_age)
    )

    # Standardize 'name' to lowercase
    def standardize_name(row):
        row['name'] = row['name'].lower() if row['name'] else None
        return row

    standarized_names = (
        clean_data
        | 'Estandarizar Nombre' >> beam.Map(standardize_name)
    )

    # Remove duplicate records based on 'id'
    dedup_data = (
        standarized_names
        | 'Create Key-Value Pairs' >> beam.Map(lambda row: (row['id'], row)) # Convertir dict a tupla para usar como clave
        | 'Group by key' >> beam.GroupByKey()
        | 'Dedup data' >> beam.Map(lambda kv: kv[1][0]) # kv is (key, list), we just want one of the values
    )

    # Use a Map transform to apply the print function to each element.
    dedup_data | 'Print Elements' >> beam.Map(print)