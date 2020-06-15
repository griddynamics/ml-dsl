# %py_load scripts/pytorch_model.py
#!/usr/bin/python
import torch
import torch.nn as nn
import torch.nn.functional as F
import time
import numpy as np
import itertools
import os
import argparse
import json
from shutil import copy2
import boto3
import io
import pandas as pd


def pretrained_embedding_layer(word_to_vec_map, word_to_index):
    vocab_len = len(word_to_index)
    emb_dim = word_to_vec_map.item().get("apple").shape[0]
    emb_matrix = np.zeros((vocab_len, emb_dim))
    for word, idx in word_to_index.items():
        emb_matrix[idx, :] = np.float32(word_to_vec_map.item().get(word))
    return emb_matrix


def accuracy(a, b):
    a = torch.argmax(a, dim=1)
    b = torch.argmax(b, dim=1)
    return torch.sum(torch.eq(a, b)).float() / list(a.size())[0]



def convert_to_one_hot(Y, C=2):
    Y = np.eye(C)[Y.reshape(-1)]
    return Y

def prepare_dataset(data, N, word_to_index):
    data['int_seq'] = data['int_seq'].apply(lambda x: [float(i) for i in x.split(',')])
    print("Max sequence is set to {}".format(N))
    data['int_seq'] = data['int_seq'].apply(lambda x: (x + [word_to_index["unk"]] * N)[:N])
    ds_x = np.asarray(list(data["int_seq"]))
    ds_y = data["class"].values
    ds_y = convert_to_one_hot(ds_y)
    return ds_x, ds_y


class LSTMModel(nn.Module):
    # predict steps is output_dim lstm_size is hidden dim of LSTM cell
    def __init__(self, word_to_vec_map, word_to_index, lstm_size=32, input_len=200):
        super(LSTMModel, self).__init__()
        torch.manual_seed(1)
        self.lstm_size = lstm_size
        self.input_len = input_len
        emb_matrix = pretrained_embedding_layer(word_to_vec_map, word_to_index)
        self.vocab_len = len(word_to_index)
        self.emb_dim = word_to_vec_map.item().get("apple").shape[0]

        self.embedding = nn.Embedding(self.vocab_len, self.emb_dim)
        self.embedding.weight = nn.Parameter(torch.from_numpy(emb_matrix))
        self.embedding.weight.requires_grad = False

        self.lstm = nn.LSTM(self.emb_dim, self.lstm_size, 2, dropout=0.5, batch_first=True)
        self.out = nn.Linear(self.lstm_size, 2)

    def forward(self, data):
        x = self.embedding(data.long())
        x, _ = self.lstm(x.view(len(data), -1, self.emb_dim))
        x = x[:, -1, :]
        x = self.out(x)
        x = F.softmax(x)
        return x


def model_fn(model_dir):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    bucket = 'mldsl-test'
    s3 = boto3.resource('s3')
    
    content_object = s3.Object(bucket, 'movie/word_to_index.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    word_to_index = json.loads(file_content)
    
    obj = s3.Object(bucket, 'movie/word_to_vec_map.npy')
    with io.BytesIO(obj.get()["Body"].read()) as f:
        word_to_vec_map = np.load(f, allow_pickle=True)
    
    model = LSTMModel(word_to_vec_map, word_to_index)
    with open(os.path.join(model_dir, 'movie.pth'), 'rb') as f:
        model.load_state_dict(torch.load(f))
    model = model.float()
    return model.to(device)


if __name__ =='__main__':
    parser = argparse.ArgumentParser()
    # hyperparameters sent by the client are passed as command-line arguments to the script.
    parser.add_argument('--epochs', type=int, default=20)
    parser.add_argument('--batch_size', type=int, default=1024)
    parser.add_argument('--learning_rate', type=float, default=0.03)
    # Data, model, and output directories
    parser.add_argument('--model_dir', type=str, default=os.environ['SM_MODEL_DIR'])
    parser.add_argument('--train', type=str, default=os.environ['SM_CHANNEL_TRAINING'])
    args, _ = parser.parse_known_args()

    np.random.seed(1)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    with open(os.path.join(args.train, "word_to_index.json"), 'r') as f:
        word_to_index = json.load(f)
    word_to_vec_map = np.load(os.path.join(args.train, "word_to_vec_map.npy"), allow_pickle=True)

    model = LSTMModel(word_to_vec_map, word_to_index, lstm_size=32, input_len=150)
    model = model.float()
    parameters = itertools.filterfalse(lambda p: not p.requires_grad, model.parameters())
    # Optimizer
    criterion = torch.nn.BCELoss()
    optimizer = torch.optim.Adam(parameters, lr=args.learning_rate)
    model.to(device)
    
    train_path = os.path.join(args.train, 'train/')
    for file in os.listdir(train_path):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(train_path, file))

    x_train, y_train = prepare_dataset(df, 150, word_to_index)
    
    tensor_x = torch.tensor(x_train).float()
    tensor_y = torch.tensor(y_train).float()
    
    print("Positive examples are {}".format(sum(tensor_y[:, 0])))
    print("Negative examples are {}".format(sum(tensor_y[:, 1])))
    del x_train
    del y_train

    traindataset = torch.utils.data.TensorDataset(tensor_x, tensor_y)
    trainloader = torch.utils.data.DataLoader(traindataset, shuffle=True, batch_size=args.batch_size)

    epochs = args.epochs
    running_loss = 0
    train_losses = []

    for epoch in range(epochs):
        start = time.time()
        for inputs, label in trainloader:
            inputs, label = inputs.to(device), label.to(device)
            optimizer.zero_grad()
            logps = model.forward(inputs)
            loss = criterion(logps, label)
            running_loss += loss.item()
            loss.backward()
            optimizer.step()
            train_losses.append(running_loss / len(trainloader))
            model.train()
        print(f"""Epoch {epoch+1}/{epochs}..  Train loss: {np.mean(train_losses):.3f}..  Accuracy: {accuracy(label, logps):.3f}..  Time: {(time.time() - start):.2f}""")
        running_loss = 0
    copy2(os.path.join(args.train, "word_to_index.json"), os.path.join(args.model_dir, "word_to_index.json"))
    copy2(os.path.join(args.train, "word_to_vec_map.npy"), os.path.join(args.model_dir, "word_to_vec_map.npy"))
    
    with open(os.path.join(args.model_dir, 'movie.pth'), 'wb') as f:
        torch.save(model.state_dict(), f)
