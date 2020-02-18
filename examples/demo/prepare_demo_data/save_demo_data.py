import argparse


from ai4ops_db import DB
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import regexp_replace, col

CPU_METRIC='cpu'
SOURCE='sensor'
KOHLS='nd-kohls-prod'

def store_to_single_file(df, output_path):
    df.coalesce(1).write.format('csv').option('header', 'true').save(output_path)


def store_metrics(metrics_path, output_path, metric_filter='%'):
    df = sql_context.read.format('csv').option('header', 'true').schema(DB.metrics_schema()).load(metrics_path)
    df = df.filter(f"metric = '{metric_filter}'")
    df = df.withColumn(DB.METRIC, regexp_replace(col(DB.METRIC), metric_filter, CPU_METRIC)).withColumn(DB.SOURCE, regexp_replace(col(DB.SOURCE), KOHLS, SOURCE))
    store_to_single_file(df, output_path)


if __name__ == "__main__":
    sc = SparkContext(appName="pepare_demo_data").getOrCreate()
    sc.addPyFile('yarn_logging.py')
    import yarn_logging
    logger = yarn_logging.YarnLogger()
    sql_context = SQLContext(sc)

    parser = argparse.ArgumentParser()
    parser.add_argument('--metrics_path', type=str, help='metrics path')
    parser.add_argument('--output_path', type=str, help='output path')
    parser.add_argument('--metric_filter', type=str, help='time string', default='%')

    args = parser.parse_args()

    store_metrics(args.metrics_path, args.output_path, args.metric_filter)