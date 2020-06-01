#!/usr/bin/python
from pyspark import SQLContext, SparkContext
from pyspark import StorageLevel
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StringType, ArrayType
from pyspark.ml.feature import Tokenizer, StopWordsRemover
import os
import argparse

def prepare_df(path, const, max_seq):
    rdd = sc.textFile(path)
    row = Row("review")
    df = rdd.map(row).toDF()
    # Clean text
    df_clean = df.select(F.lower(F.regexp_replace(F.col('review'), "[^a-z\\s]", "")).alias('review'))
    # Tokenize text
    tokenizer = Tokenizer(inputCol='review', outputCol='words_token')
    df_words_token = tokenizer.transform(df_clean).select('words_token')
    #Remove stop words
    remover = StopWordsRemover(inputCol='words_token', outputCol='words_clean')
    df_words_no_stopw = remover.transform(df_words_token).select('words_clean')
    #Filter length word > 3
    filter_length_udf = F.udf(lambda row: [x for x in row if len(x) >= 3], ArrayType(StringType()))
    df_stemmed = df_words_no_stopw.withColumn('words_stemmed', filter_length_udf(F.col('words_clean')))
    #Filter length of review < max_seq
    filter_length_udf = F.udf(lambda row: row if len(row)<=max_seq else row[:max_seq], ArrayType(StringType()))
    df_stemmed = df_words_no_stopw.withColumn('words_stemmed', filter_length_udf(F.col('words_clean')))
    return df_stemmed.withColumn("class", F.lit(const))

def prepare_int_seq(words_df, exploded_df, path_to_save):
    jdf = exploded_df.join(words_df.select("words_stemmed", "id"), on=['words_stemmed'], how='left').fillna(0)
    qdf=jdf.groupBy('review_id').agg(F.collect_list("id").alias("int_seq"), F.max("class").alias("class"))
    qdf = qdf.withColumn('int_seq', F.concat_ws(',', 'int_seq'))
    qdf.coalesce(1).write.format('csv').option('header', 'true').save(path_to_save)
        
if __name__ == '__main__':
        parser = argparse.ArgumentParser()
        parser.add_argument('--train_pos_path', type=str, help="train positive reviews path")
        parser.add_argument('--train_neg_path', type=str, help="train negative reviews path")
        parser.add_argument('--test_pos_path', type=str, help="test positive reviews path")
        parser.add_argument('--test_neg_path', type=str, help="test negative reviews path")
        parser.add_argument('--max_seq', type=int, help="Words sequence size")
        parser.add_argument('--output_path', type=str, help="Sequences output path")
        reviews_filter = '*.txt'

        args, d = parser.parse_known_args()
        output_path = args.output_path
        SparkContext.setSystemProperty('spark.sql.broadcastTimeout', '36000')
        sc = SparkContext(appName="word_tokenizer").getOrCreate()
        sql = SQLContext(sc)
        
        df_pos = prepare_df(f"{args.train_pos_path}/{reviews_filter}", 1, args.max_seq)  
        df_neg = prepare_df(f"{args.train_neg_path}/{reviews_filter}", 0, args.max_seq)  
        df = df_pos.union(df_neg)
        w = Window.orderBy(["words_stemmed"]) 
        df = df.withColumn("review_id", F.row_number().over(w))
        exploded_df = df.select("review_id", "class",
                                F.explode(F.col('words_stemmed')).alias('words_stemmed'))
        words_df=exploded_df.select("review_id", "words_stemmed").groupBy('words_stemmed').count()
 
        w = Window.orderBy(["words_stemmed"]) 
        words_df = words_df.withColumn("id", F.row_number().over(w))
        words_path = '{}/words'.format(output_path)
        (words_df.select(F.col('id'), F.col('words_stemmed'), F.col("count"))
         .coalesce(1)
         .write
         .format('csv')
         .option('header', 'true')
         .save(words_path))
        print('Words to: "{}"'.format(words_path))
        
        exploded_df.persist(StorageLevel.MEMORY_AND_DISK)
        words_df.persist(StorageLevel.MEMORY_AND_DISK)
        train_path = '{}/train'.format(output_path)
        prepare_int_seq(words_df, exploded_df, train_path)
        print('Train split to: "{}"'.format(train_path))
        
        df_pos = prepare_df(f"{args.train_pos_path}/{reviews_filter}", 1, args.max_seq)  
        df_neg = prepare_df(f"{args.train_neg_path}/{reviews_filter}", 0, args.max_seq) 
        df = df_pos.union(df_neg)
        df = df.withColumn("review_id", F.row_number().over(w))
        exploded_df_test = df.select("review_id", "class", F.explode(F.col('words_stemmed')).alias('words_stemmed'))
        exploded_df_test.persist(StorageLevel.MEMORY_AND_DISK)
        test_path = '{}/test'.format(output_path)
        prepare_int_seq(words_df, exploded_df_test, test_path)
        print('Test split to: "{}"'.format(test_path))
