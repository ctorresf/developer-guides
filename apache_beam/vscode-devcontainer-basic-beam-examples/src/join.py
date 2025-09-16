import apache_beam as beam
import json

# Sample data
customers = [
    {'customer_id': 'C1', 'name': 'Ana'},
    {'customer_id': 'C2', 'name': 'Juan'} # Customer without orders
]
orders = [
    {'customer_id': 'C1', 'orden_id': 'O1', 'amount': 50},
    {'customer_id': 'C1', 'orden_id': 'O2', 'amount': 75},
    {'customer_id': 'C3', 'orden_id': 'O3', 'amount': 100} # Order without customer
]

with beam.Pipeline() as pipeline:
    # 1. Convert to key-value pairs
    customers_kv = (
        pipeline
        | 'Create customers' >> beam.Create(customers)
        | 'customers to KV' >> beam.Map(lambda c: (c['customer_id'], c))
    )
    orders_kv = (
        pipeline
        | 'Create Orders' >> beam.Create(orders)
        | 'Orders to KV' >> beam.Map(lambda o: (o['customer_id'], o))
    )
    # 2. Group pairs by key
    join_result = (
        {"customers": customers_kv, "orders":orders_kv}
        | 'Unir Datos' >> beam.CoGroupByKey()
    )
    # The result of join_result looks like this:
    # ["C1", {"customers": [customers], "orders": [orders]}]
    # ["C2", {"customers": [customers], "orders": []}]
    # ["C3", {"customers": [], "orders": [orders]}]

    # 3. Process the grouped data to perform an Inner Join
    def process_join(element):
        customer_id, grouped = element
        customer_info = grouped['customers']
        orders_list = grouped['orders']

        # Inner Join: only if there is one customer and one or more orders
        if customer_info and orders_list:
            for order in orders_list:
                joined_row = {
                    'customer_id': customer_id,
                    'name_client': customer_info[0]['name'],
                    'orden_id': order['orden_id'],
                    'amount': order['amount']
                }
                yield joined_row

    final_result = (
        join_result
        | 'Crear Filas Unidas' >> beam.ParDo(process_join)
    )

    # 4. Use a Map transform to apply the print function to each element.
    ( final_result 
        | 'Convert to JSON' >> beam.Map(
                lambda row: json.dumps(row, ensure_ascii=False)
            )
        | 'Print Elements' >> beam.Map(print)
    )

    # Final result of an Inner Join:
    # {"customer_id": "C1", "name_client": "Ana", "orden_id": "O1", "amount": 50}
    # {"customer_id": "C1", "name_client": "Ana", "orden_id": "O2", "amount": 75}