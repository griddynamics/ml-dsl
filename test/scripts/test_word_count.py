#!/usr/bin/python
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path_to_book', help='s3 path to book')
    parser.add_argument('--path_to_save', help='s3 path to save')
    parser.add_argument('--output_path', help='s3 path to save')
    args = parser.parse_known_args()
    sc = SparkContext(appName="word_count").getOrCreate()
    sc._jsc.hadoopConfiguration().set("fs.s3.access.key", "AKIAYTWQCSGE3V2KMWVH")
    sc._jsc.hadoopConfiguration().set("fs.s3.secret.key", "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")
    sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem")
    lines = sc.textFile(args[0].path_to_book)
    counts = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
    words_path = '{}{}/'.format(args[0].path_to_save, args[0].output_path)
    counts.saveAsTextFile(words_path)
    print('Word counts to: "{}"'.format(words_path))
    sc.stop()
