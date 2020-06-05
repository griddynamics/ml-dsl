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
from com.griddynamics.dsl.ml.mldsl import Platform


py_path = "-n sd_streaming_ingest_to_es.py -p test     -pm 1 -o /opt --x 12 " \
          "     --y 23"
parser = argparse.ArgumentParser()
parser.add_argument('--platform', '-pm', type=Platform, help='Working platform')
parser.add_argument('--name', '-n', type=str, help='Name of script file', default='default.py')
parser.add_argument('--profile', '-p', type=str, help='Name of parameters profile', default='DemoProfile')
parser.add_argument('--output_path', '-o', type=str, help='Output GCS path', default='')
args = parser.parse_known_args(py_path.split())
print(args)
