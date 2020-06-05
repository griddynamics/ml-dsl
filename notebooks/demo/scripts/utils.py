#!/usr/bin/python

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from tensorflow.python.lib.io import file_io
import pandas as pd
import numpy as np


def read_csv(path):
    files = file_io.get_matching_files('{}/part*'.format(path))
    pdf = []
    for file in files:
        with file_io.FileIO(file, 'r') as f:
            df = pd.read_csv(f)
            if df is not None and len(df) != 0:
                pdf.append(df)
    if len(pdf) == 0:
        return None
    return pd.concat(pdf, axis=0, ignore_index=True).reset_index()


def prepare_dataset(path, N, word_to_index):
    data = read_csv(path)
    data.dropna(inplace=True)
    data['int_seq'] = data['int_seq'].apply(lambda x: [int(i) for i in x.split(',')])
    l = data['int_seq'].apply(lambda x: len(x))
    print("Max sequence is set to {}".format(N))
    data['int_seq'] = data['int_seq'].apply(lambda x: (x + [word_to_index["unk"]] * N)[:N])
    ds_x = np.asarray(list(data["int_seq"]))
    ds_y = data["class"].values
    return ds_x, ds_y, l


def read_glove_vectors(glove_file):
    files = file_io.get_matching_files('{}/part*'.format(glove_file))
    for file in files:
        with file_io.FileIO(file, 'r') as f:
            word_to_vec_map = {}
            words_to_index = {}
        fl = f.readline()
        for line in f:
            line = line.strip().split('\t')
            word_to_vec_map[line[0]] = np.array(line[1].split(','), dtype=np.float64)
            words_to_index[line[0]] = int(line[2])
    return words_to_index, word_to_vec_map
