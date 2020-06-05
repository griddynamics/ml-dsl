# Copyright (c) 2020 Grid Dynamics International, Inc. All Rights Reserved
# http://www.griddynamics.com
# Classification level: PUBLIC
# Licensed under the Apache License, Version 2.0(the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE - 2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Id:          ML_PLATFORM
# Project:     ML Platform
# Description: DSL to configure and execute ML/DS pipelines

import argparse
import random
from datetime import datetime

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# Define a callable source 
def generate_rdd(sc):
    reading = []
    for i in range(5):
        sensor_id = random.randint(1,100)
        reading.append(("sensor_" + str(sensor_id), random.random() * 1000,
                        int((datetime.now().timestamp()))))
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
