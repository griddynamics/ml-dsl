import torch
import torch.nn as nn
import torch.nn.functional as F
import time
import numpy as np
import itertools
import os
import argparse
import json


def pretrained_embedding_layer(word_to_vec_map, word_to_index):
    vocab_len = len(word_to_index)
    emb_dim = word_to_vec_map.item().get("apple").shape[0]
    emb_matrix = np.zeros((vocab_len, emb_dim))
    for word, idx in word_to_index.items():
        emb_matrix[idx, :] = word_to_vec_map.item().get(word)
    return emb_matrix


def accuracy(a, b):
    a = torch.argmax(a, dim=1)
    b = torch.argmax(b, dim=1)
    return torch.sum(torch.eq(a, b)).float() / list(a.size())[0]


class LSTMModel(nn.Module):
    # predict steps is output_dim lstm_size is hidden dim of LSTM cell
    def __init__(self, word_to_vec_map, word_to_index, lstm_size=1024, input_len=150):
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
    model = LSTMModel()
    with open(os.path.join(model_dir, 'movie.pth'), 'rb') as f:
        model.load_state_dict(torch.load(f))
    return model.to(device)


if __name__ =='__main__':
    parser = argparse.ArgumentParser()
    # hyperparameters sent by the client are passed as command-line arguments to the script.
    parser.add_argument('--epochs', type=int, default=15)
    parser.add_argument('--batch_size', type=int, default=1024)
    parser.add_argument('--learning_rate', type=float, default=0.015)
    # Data, model, and output directories
    parser.add_argument('--model_dir', type=str, default=os.environ['SM_MODEL_DIR'])
    parser.add_argument('--train', type=str, default=os.environ['SM_CHANNEL_TRAINING'])
    args, _ = parser.parse_known_args()

    np.random.seed(1)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    with open(args.train + '/'+ "word_to_index.json", 'r') as f:
        word_to_index = json.load(f)
    word_to_vec_map = np.load(args.train + '/'+ "word_to_vec_map.npy", allow_pickle=True)

    model = LSTMModel(word_to_vec_map, word_to_index)
    model = model.float()
    parameters = itertools.filterfalse(lambda p: not p.requires_grad, model.parameters())
    # Optimizer
    criterion = torch.nn.BCELoss()
    optimizer = torch.optim.Adam(parameters, lr=args.learning_rate)
    model.to(device)

    x_train = np.load(args.train + '/'+ 'data_train.npy', allow_pickle=True)
    y_train = np.load(args.train + '/'+ 'label_train.npy', allow_pickle=True)
    tensor_x = torch.tensor(x_train[:, :200]).float()
    tensor_y = torch.tensor(y_train).float()

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
        print("""Epoch {}/{}.. Train loss: {}.. Accuracy: {}.. Time: {}""".format(epoch+1, epochs,
                                                                                  round(np.mean(train_losses),3),
                                                                                  round(accuracy(label, logps), 3),
                                                                                  round((time.time() - start),2)))
        running_loss = 0
    with open(os.path.join(args.model_dir, 'movie.pth'), 'wb') as f:
        torch.save(model.state_dict(), f)

