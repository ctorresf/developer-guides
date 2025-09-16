import apache_beam as beam
import json

# Small table with products (Side Input)
products = [
    {'id_product': 101, 'name': 'Keyboard', 'category': 'Electronics'},
    {'id_product': 102, 'name': 'Book', 'category': 'Books'}
]

# Large table with transactions (main PCollection)
transactions = [
    {'id_transaction': 'TX01', 'id_product': 101},
    {'id_transaction': 'TX02', 'id_product': 102},
    {'id_transaction': 'TX03', 'id_product': 101}
]

with beam.Pipeline() as pipeline:
    products_side_input = (pipeline 
                           | 'Create Products' >> beam.Create(products)
                           | 'Map to KV' >> beam.Map(lambda row: (row['id_product'], row))
    )
    transactions_main = pipeline | 'Create Transactions' >> beam.Create(transactions)

    # Use of Side Input ro enrich the main PCollection
    def enrich_transaction(transaction, products_dict):
        product_info = products_dict.get(transaction['id_product'])
        if product_info:
            transaction['name_producto'] = product_info['name']
            transaction['category'] = product_info['category']
        return transaction

    enriched_data = (
        transactions_main
        | 'Enrich Data' >> beam.Map(
            enrich_transaction,
            products_dict=beam.pvalue.AsDict(products_side_input)
        )
    )

    # Use a Map transform to apply the print function to each element.
    ( enriched_data 
        | 'Convert to JSON' >> beam.Map(
                lambda row: json.dumps(row, ensure_ascii=False)
            )
        | 'Print Elements' >> beam.Map(print)
    )