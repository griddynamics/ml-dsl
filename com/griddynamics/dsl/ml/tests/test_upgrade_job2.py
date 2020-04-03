import argparse
import json
import random 
import time
from datetime import datetime, timedelta

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
    parser = argparse.ArgumentParser()
    parser.add_argument('--wait_delay', type=int, help='time between attempts')
    parser.add_argument('--wait_tries', type=int, help='number of attempts')
    parser.add_argument('--batch_interval', type=int, help='batch interval for streaming job')

    args = parser.parse_known_args()

    ssc = StreamingContext(sc, args[0].batch_interval)
    srs = [generate_rdd(sc) for i in range(1000)]
    lines = ssc.queueStream(srs)
    filtered = lines.filter(lambda x: x[1]<300)
    filtered.pprint()
    print("NEW JOB successfully started")
    ssc.start()
    ssc.awaitTermination()
