import argparse
import random 
import time
from datetime import datetime

from pyspark import SparkContext
from pyspark.streaming import DStream, StreamingContext

# Define a callable source 
def generate_rdd(sc):
    reading = []
    for i in range(5):
        sensor_id = random.randint(1,100)
        reading.append(("sensor_" + str(sensor_id), random.random() * 1000, int((datetime.now().timestamp()))))
    return sc.parallelize(reading)

if __name__ == '__main__':
    sc = SparkContext(appName="test_new_functionality").getOrCreate()
    #sc._jsc.hadoopConfiguration().set("fs.s3n.access.key", "AKIAYTWQCSGE3V2KMWVH")
    #sc._jsc.hadoopConfiguration().set("fs.s3n.secret.key", "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")
    #sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3n.S3FileSystem")
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--path_to_save', help='s3 path to save')
    parser.add_argument('--output_path', help='s3 path to save')
    
    args = parser.parse_known_args()

    ssc = StreamingContext(sc, 1)
    srs = [generate_rdd(sc) for i in range(50)]
    lines = ssc.queueStream(srs)
    filtered = lines.filter(lambda x: x[1]<300)
    words_path = '{}{}/'.format(args[0].path_to_save, args[0].output_path)
    filtered.saveAsTextFiles(words_path)
    print("NEW JOB successfully started")
    ssc.start()
    ssc.awaitTermination()
