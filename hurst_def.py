import numpy as np
from numpy.typing import NDArray

import pandas as pd

import seaborn as sns
import matplotlib.pyplot as plt

import os
#%% include sample data

data_1 = pd.read_csv(os.path.join(os.path.dirname(__file__), "BNBUSDT.csv")).values
data_1 = data_1[:, 1].astype("float32")
data_1 = data_1.reshape(-1, 1)

#%% for Plotting purposes, eyeballing possible Mean Resvision tendencies

sns.lineplot(data_1)
plt.show()

#%%

times = np.array([50, 100, 250, 1000])  #intervals for the Hurst coefficient, may also choose based on time frames e.g. 120 in minute for 2h 

def get_hurst(data: NDArray, intervals: NDArray):
    data = data.reshape(-1)
    data = np.diff(np.log(data))
    
    average_range = []
    
    for interval in intervals:
        intervals_data = []
        
        for i in range(len(data)//interval):
            
            start = i * interval
            border = i + interval
            
            block = data[start : border]
            demeaned_data = block - np.mean(block)
            standard_dev = np.std(demeaned_data)
            
            if standard_dev == 0:
                break
            
            y_k = np.cumsum(demeaned_data)
            
            range_ = np.max(y_k) - np.min(y_k)
            
            rescaled = range_ / standard_dev
            intervals_data.append(rescaled)
            
        meaned = np.mean(intervals_data)
        average_range.append(meaned)
      
    
    arrayed_range = np.array(average_range)
        
    y = np.log(np.array(arrayed_range).reshape(-1, 1))  #log and reshape it
    x = np.log(intervals.reshape(-1))                   #reshape Intervals 

    x = np.stack([np.ones(len(x)), x]).T                #Include intercept

    beta = np.linalg.inv(x.T @ x) @ (x.T @ y)           #fit the beta and return it as "Hurst"
    return beta[1]
    
    
    

#%%
get_hurst(data_1, times)











