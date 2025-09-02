#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 14:44:34 2025

@author: maximilianbechthold
"""

import pandas as pd
import numpy as np

import seaborn as sns
import matplotlib.pyplot as plt

import keras
from keras import layers


import os
#%%

data_1 = pd.read_csv(os.path.join(os.path.dirname(__file__), "BNBUSDT.csv")).values
data_1 = data_1[:, 1].astype("float32")
data_1 = data_1.reshape(-1, 1)


#%%
#data_1 = pd.read_csv("BNBUSDT.csv").values
data_1_test = data_1[-100000:, :]
raw_data = data_1[:100000, :]


raw_data.shape

#%%

def make_sequentials(data, timelag, return_structure = False):
    data = data[:, 0]
    data = data.reshape(-1, 1)
    inputs = []
    list_target = []

    means = []
    stds = []
    
    i = timelag
    
    while i < len(data):
        
        training = data[i-timelag : i]
        target = data[i]
        
        mean = np.mean(training)
        sd = np.std(training)
        
        if sd == 0:
            sd = 1
            
        training = (training - mean) / sd
        target = (target - mean) / sd

        inputs.append(training)
        list_target.append(target)
        means.append(mean)
        stds.append(sd)

        i = i + 1
    
    if return_structure == False:
        return np.stack(inputs), np.stack(list_target)
    else:
        return np.stack(inputs), np.stack(list_target), np.stack(means), np.stack(stds)


def train_test_split(X, y, quota):
    if len(X) != len(y):
        print("Length Mismatch of X: ", len(X), " and y: ", len(y))
    else:
        cutoff = round(len(X) * quota)
        X_train = X[:cutoff].astype("float32")
        y_train = y[:cutoff].reshape(-1, 1).astype("float32")
        
        X_test = X[cutoff:].astype("float32")
        y_test = y[cutoff:].reshape(-1, 1).astype("float32")
    
        return X_train, y_train, X_test, y_test


X, y = make_sequentials(raw_data, 60)

#%%
X_train, y_train, X_test, y_test = train_test_split(X, y, 0.7)
X_test.shape

#%%
model = keras.Sequential([
    layers.Input(shape=(X_train.shape[1], X_train.shape[2])),

    layers.Conv1D(
        filters=32,
        kernel_size=10,
        padding="same", 
        strides = 1,
        activation="relu"
    ),
    
    layers.Conv1D(
        filters=32,
        kernel_size=10,
        padding="same", 
        strides = 1,
        activation="relu"
    ),


    
    layers.Conv1D(
        filters=32,
        kernel_size=10,
        padding="same", 
        strides = 1,
        activation="relu"
    ),

    layers.Flatten(),

    layers.Dense(32, activation="relu"),
    layers.Dense(1)
    
    
])

model.compile(optimizer = keras.optimizers.Adam(learning_rate=0.005), loss = "mse", metrics=["mae"])
model.summary()


#%%
history = model.fit(
    X_train, 
    y_train,
    validation_split=0.2,   # use 20% of training set for validation
    epochs=50,              # adjust upwards if no overfitting
    batch_size=32,          # common default, tune later
    verbose=1,
    shuffle=False,
    callbacks = [keras.callbacks.EarlyStopping(monitor = "val_loss", patience = 20, mode = "min")]
)

plt.plot(history.history["loss"], label="Training Loss")
plt.plot(history.history["val_loss"], label="Validation Loss")
plt.legend()
plt.show()

#%%

X, y, mean, std = make_sequentials(data_1_test, 60, return_structure=True)

y_pred = model.predict(X).reshape(-1)
y = y.reshape(-1)

#%%

y_pred = y_pred * std + mean
y_true = y * std + mean

plt.plot(y_pred[-70:], label = "Prediction")
plt.plot(y_true[-70:], label = "Target")
plt.legend()
plt.show()

print("Ich bin nicht gestorben")
#%%
trend = y_true[1:] - y_true[:-1]
wrong = y_pred - y_true
wrong = wrong[:-1]


#%%
print(wrong.shape)
print(trend.shape)

#%%




