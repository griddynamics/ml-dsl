#!/usr/bin/python

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import pandas as pd
    import numpy as np
    from tensorflow.python.lib.io import file_io
    from tensorflow.keras.layers import Dense, Input, LSTM, Embedding, Dropout, Activation
    from tensorflow.keras.models import Model
    from tensorflow.keras.callbacks import Callback
    from tensorflow.keras.optimizers import Adam
    import tensorflow as tf

    #from tensorflow.core.protobuf import rewriter_config_pb2
    #from tensorflow.keras.backend import set_session
    #tf.keras.backend.clear_session()  # For easy reset of notebook state.

    from uuid import uuid4
    import argparse
    import matplotlib
    if matplotlib.get_backend() in ['TkAgg', 'TkCairo']:
        matplotlib.use('agg')
    import matplotlib.pyplot as plt
    import seaborn as sns

tf.get_logger().setLevel('INFO')
class MetricCallback(Callback):
    def on_train_begin(self,logs={}):
        self.losses = []
        self.accuracies = []

    def on_batch_end(self, batch, logs={}):
        self.losses.append(logs.get('loss'))
        self.accuracies.append(logs.get('acc'))


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


def pretrained_embed_layer(word_to_vec_map, word_to_index):
    vocab_len = len(word_to_index) + 1
    emb_dim = word_to_vec_map["cucumber"].shape[0]
    emb_matrix = np.zeros((vocab_len, emb_dim))
    for word, idx in word_to_index.items():
        emb_matrix[idx, :] = word_to_vec_map[word]
    embedding_layer = Embedding(input_dim=vocab_len, trainable=False, 
                                output_dim=emb_dim)
    embedding_layer.build((None,))
    embedding_layer.set_weights([emb_matrix])
    return embedding_layer


def define_model(input_shape, word_to_vec_map, word_to_index, rnn_units, dropout=0.5):
    sentence_indices = Input(input_shape, dtype="int32")
    # Create the embedding layer pretrained with GloVe Vectors
    embedding_layer = pretrained_embed_layer(word_to_vec_map, word_to_index)
    # Propagate sentence_indices through your embedding layer
    embeddings = embedding_layer(sentence_indices)
    X = LSTM(units=rnn_units, return_sequences=False)(embeddings)
    # Add dropout with a probability 
    X = Dropout(dropout)(X)
    # Propagate X through a Dense layer
    X = Dense(2)(X)
    # Add a softmax activation
    X = Activation("softmax")(X)
    # Create Model instance which converts sentence_indices into X.
    model = Model(inputs=sentence_indices, outputs=X)
    return model


def convert_to_one_hot(Y, C=2):
    Y = np.eye(C)[Y.reshape(-1)]
    return Y

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


def plot_metrics(callback, dir_to_save):
    f, axes = plt.subplots(1, 2, figsize=(18, 5))
    plt.style.use('seaborn')
    plt.rcParams['axes.titlesize'] = 16
    sns.lineplot(x=range(len(callback.losses)), y=callback.losses, ax=axes[0])
    axes[0].title.set_text("Loss")
    sns.lineplot(x=range(len(callback.accuracies)), y=callback.accuracies, ax=axes[1])
    axes[1].title.set_text("Accuracy")
    plt.tight_layout(.5)
    plt.savefig('{}'.format(dir_to_save))
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--train_path', type=str, help="Train files path")
    parser.add_argument('--output_path', type=str, help="Models output path")
    parser.add_argument('--word_embeds', type=str, help="Models output path")
    parser.add_argument('--seq_len', type=int, help="Length of input sequence")
    parser.add_argument('--epochs', type=int, help="Number of epochs")
    args, d = parser.parse_known_args()

    word_to_index, word_to_vec_map = read_glove_vectors(args.word_embeds)
    N = args.seq_len
    train_x, train_y, l = prepare_dataset(args.train_path, N, word_to_index)
    train_y = convert_to_one_hot(train_y, C=2)
    NUM_EPOCS = args.epochs
    RNN_STATE_DIM = 32
    LEARNING_RATE = 0.01

    #config_proto = tf.ConfigProto()
    #off = rewriter_config_pb2.RewriterConfig.OFF
    #config_proto.graph_options.rewrite_options.arithmetic_optimization = off
    #session = tf.Session(config=config_proto)
    #set_session(session)

    model = define_model((N, ), word_to_vec_map, word_to_index, RNN_STATE_DIM)
    print(model.summary())
    model.compile(loss='binary_crossentropy', optimizer=Adam(lr=LEARNING_RATE), 
                  metrics=['accuracy'])
    # fit model
    metrics = MetricCallback()
    model.fit(train_x, train_y, 
                  batch_size=1024, epochs=NUM_EPOCS, 
                  callbacks=[metrics], shuffle=True)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        # save the model to file
        local_dir = uuid4().hex
        file_io.recursive_create_dir(local_dir)
        local_path = '{}/saved_model'.format(local_dir)
        tf.saved_model.save(model, f'{local_dir}/saved_model/')
        local_path_chart = '{}/metrics.png'.format(local_dir)
        plot_metrics(metrics, local_path_chart)
    
        remote_dir = args.output_path
        remote_path_chart = '{}/metrics.png'.format(remote_dir)
        if not remote_dir.startswith('gs://'):
            file_io.recursive_create_dir(remote_dir)
        tf.saved_model.save(model, '{}/'.format(remote_dir))
        file_io.copy(local_path_chart, remote_path_chart)
        file_io.delete_recursively(local_dir)
