# %py_load notebooks/demo/scripts/text_tokenizer.py
#!/usr/bin/python
from pyspark import SQLContext, SparkContext
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.types import StringType, ArrayType, IntegerType, FloatType
from pyspark.ml.feature import Tokenizer
import argparse


def read_glove_vecs(glove_file, output_path):
    rdd = sc.textFile(glove_file)
    row = Row("glovevec")
    df = rdd.map(row).toDF()
    split_col = F.split(F.col('glovevec'), " ")
    df = df.withColumn('word', split_col.getItem(0))
    df = df.withColumn('splitted', split_col)
    vec_udf = F.udf(lambda row: [float(i) for i in row[1:]], ArrayType(FloatType()))
    df = df.withColumn('vec', vec_udf(F.col('splitted')))
    df = df.drop('splitted', "glovevec")
    w = Window.orderBy(["word"])
    qdf = df.withColumn('vec', F.concat_ws(',', 'vec')).withColumn("id", F.row_number().over(w))
    
    path = '{}/words'.format(output_path)
    qdf.coalesce(1).write.format('csv').option("sep","\t").option('header', 'true').save(path)
    print('Words saved to: "{}"'.format(path))
    list_words = list(map(lambda row: row.asDict(), qdf.collect()))
    word_to_vec_map = {item['word']: item['vec'] for item in list_words}
    words_to_index = {item['word']:item["id"] for item in list_words}
    index_to_words = {item["id"]: item['word'] for item in list_words}
    return words_to_index, index_to_words, word_to_vec_map


def prepare_df(path, const, words_dct):
    rdd = sc.textFile(path)
    row = Row("review")
    df = rdd.map(row).toDF()
    # Clean text
    df_clean = df.select(F.lower(F.regexp_replace(F.col('review'), "n't", " n't")).alias('review'))
    df_clean = df_clean.select(F.lower(F.regexp_replace(F.col('review'), "[^0-9a-zA-Z\\s]", "")).alias('review'))
    # Tokenize text
    tokenizer = Tokenizer(inputCol='review', outputCol='words_token')
    df_words_token = tokenizer.transform(df_clean).select('words_token')
    df_cutted = df_words_token.withColumn('length', F.size(F.col('words_token')))
    # Replace word with it's index
    word_udf = F.udf(lambda row: [words_to_index[w] if w in words_to_index.keys() else words_to_index["unk"] for w in row],
                 ArrayType(IntegerType()))
    df_stemmed = df_cutted.withColumn('words_stemmed', word_udf(F.col('words_token')))
    return df_stemmed.withColumn("class", F.lit(const))


def save_dataset(df_pos, df_neg, path):
    df = df_pos.union(df_neg)
    w = Window.orderBy(["words_stemmed"])
    df = df.withColumn("review_id", F.row_number().over(w)).withColumn('int_seq',
                                                                       F.concat_ws(',', 'words_stemmed'))
    qdf = df.select(['review_id', 'int_seq', 'class'])
    qdf.coalesce(1).write.format('csv').option('header', 'true').save(path)

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--train_pos_path', type=str, help="train positive reviews path")
    parser.add_argument('--train_neg_path', type=str, help="train negative reviews path")
    parser.add_argument('--test_pos_path', type=str, help="test positive reviews path")
    parser.add_argument('--test_neg_path', type=str, help="test negative reviews path")
    parser.add_argument('--word_embeds', type=str, help="Path to glove word embeddings")
    parser.add_argument('--output_path', type=str, help="Sequences output path")
    reviews_filter = '99*.txt'

    args, d = parser.parse_known_args()
    output_path = args.output_path
    SparkContext.setSystemProperty('spark.sql.broadcastTimeout', '36000')
    sc = SparkContext(appName="word_tokenizer").getOrCreate()
    sql = SQLContext(sc)

    words_to_index, index_to_words, word_to_vec_map = read_glove_vecs(args.word_embeds, output_path)
    
    df_pos = prepare_df(f"{args.train_pos_path}/{reviews_filter}", 1, words_to_index)
    df_neg = prepare_df(f"{args.train_neg_path}/{reviews_filter}", 0, words_to_index)
    train_path = '{}/train'.format(output_path)
    save_dataset(df_pos, df_neg, train_path)
    print('Train saved to: "{}"'.format(train_path))

    df_pos = prepare_df(f"{args.train_pos_path}/{reviews_filter}", 1, words_to_index)
    df_neg = prepare_df(f"{args.train_neg_path}/{reviews_filter}", 0, words_to_index)
    test_path = '{}/test'.format(output_path)
    save_dataset(df_pos, df_neg, test_path)
    print('Test saved to: "{}"'.format(test_path))
