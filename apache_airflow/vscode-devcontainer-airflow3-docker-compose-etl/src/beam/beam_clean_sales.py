import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyarrow as pa
import os

# Definimos el esquema de salida para el archivo Parquet usando PyArrow
parquet_schema = pa.schema([
    ('id_venta', pa.string()),
    ('fecha', pa.string()),
    ('sucursal_id', pa.string()),
    ('monto', pa.float64()),
    ('estado_sucursal', pa.string()),
    ('region', pa.string())
])

def parse_csv(element):
    """Parsea una línea CSV simple eliminando espacios."""
    return [field.strip() for field in element.split(',')]

class CleanAndJoinData(beam.PTransform):
    """Transformación compuesta para cruzar Ventas y Logs de Sucursales."""
    def expand(self, pcollections):
        sales_pcoll = pcollections['sales']
        branches_pcoll = pcollections['branches']

        # Paso A: Procesar Ventas -> (sucursal_id, dict_datos_venta)
        keyed_sales = (
            sales_pcoll 
            | 'ParseSales' >> beam.Map(parse_csv)
            | 'FilterHeaderSales' >> beam.Filter(lambda x: x[0] != 'id_venta')
            | 'KeySalesByBranch' >> beam.Map(lambda x: (x[2], {
                'id_venta': x[0],
                'fecha': x[1],
                'monto': float(x[3])
            }))
        )

        # Paso B: Procesar Sucursales -> (sucursal_id, dict_datos_sucursal)
        keyed_branches = (
            branches_pcoll
            | 'ParseBranches' >> beam.Map(parse_csv)
            | 'FilterHeaderBranches' >> beam.Filter(lambda x: x[0] != 'sucursal_id')
            | 'KeyBranches' >> beam.Map(lambda x: (x[0], {
                'estado_sucursal': x[1],
                'region': x[2]
            }))
        )

        # Paso C: CoGroupByKey (Join) por sucursal_id
        def merge_joined_records(element):
            sucursal_id, data = element
            sales_list = data['sales']
            branch_list = data['branches']
            
            # Si la sucursal no tiene logs, asignamos valores por defecto
            branch_info = branch_list[0] if branch_list else {'estado_sucursal': 'DESCONOCIDO', 'region': 'DESCONOCIDA'}
            
            # Generar un registro plano por cada venta
            joined_rows = []
            for sale in sales_list:
                joined_rows.append({
                    'id_venta': sale['id_venta'],
                    'fecha': sale['fecha'],
                    'sucursal_id': sucursal_id,
                    'monto': sale['monto'],
                    'estado_sucursal': branch_info['estado_sucursal'],
                    'region': branch_info['region']
                })
            return joined_rows

        return (
            {'sales': keyed_sales, 'branches': keyed_branches}
            | 'MergeCollections' >> beam.CoGroupByKey()
            | 'FlattenResults' >> beam.FlatMap(merge_joined_records)
        )

def run():
    # Rutas dentro del contenedor
    input_sales = '/opt/airflow/data/bronze/ventas.csv'
    input_branches = '/opt/airflow/data/bronze/sucursales.csv'
    output_path = '/opt/airflow/data/silver/ventas_consolidadas' # Beam añade extensión/sufijos

    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        sales = p | 'ReadSales' >> beam.io.ReadFromText(input_sales)
        branches = p | 'ReadBranches' >> beam.io.ReadFromText(input_branches)

        # Ejecución del pipeline compuesto
        joined_data = {'sales': sales, 'branches': branches} | 'CleanAndJoin' >> CleanAndJoinData()

        # Escritura en formato Parquet
        (
            joined_data 
            | 'WriteToParquet' >> beam.io.parquetio.WriteToParquet(
                file_path_prefix=output_path,
                schema=parquet_schema,
                file_name_suffix='.parquet',
                shard_name_template='' # Evita fragmentación en múltiples archivos para el ejercicio
            )
        )

if __name__ == '__main__':
    run()