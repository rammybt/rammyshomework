# -*- coding: utf-8 -*-
"""
Created on Sun Oct  7 22:07:49 2018

@author: Ramkumar.Ranganathan
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pandas.plotting import autocorrelation_plot
color = sns.color_palette()
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error
from sklearn.metrics import accuracy_score
import numpy as np
import sqlalchemy
import pyodbc
import urllib
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import seaborn as sns
from pandas.plotting import autocorrelation_plot
color = sns.color_palette()
from statsmodels.tsa.arima_model import ARIMA
from pandas import datetime
from sklearn.metrics import mean_squared_error
from sklearn.metrics import accuracy_score
import numpy as np

def parser(x):
    return datetime.strptime(x, '%Y-%m')

connString = "Driver={SQL Server Native Client 11.0};Server=IN2104950W2;Database=NSEData;trusted_connection=yes;"
params = urllib.parse.quote_plus(connString)
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

sql = '''
SELECT [Date]
      ,[Close]
  FROM [NSEData].[dbo].[IndexHistory]
  where [Index Name] = 'NIFTY 50'
'''
df = pd.read_sql_query(sql, engine)

df.index = df['Date'].apply(pd.to_datetime)
del df['Date']

df = df.fillna(df.bfill())
df = df['Close'].resample('MS').mean()

df.plot()
plt.show()

autocorrelation_plot(df)
plt.show()

quantity = df.values
size = int(len(quantity) * 0.66)
train, test = quantity[0:size], quantity[size:len(quantity)]
history = [x for x in train]
predictions = list()

for t in range(len(test)):
    model = ARIMA(history, order=(5 ,2 ,0))
    model_fit = model.fit(disp=0)
    output = model_fit.forecast()
    yhat = output[0]
    predictions.append(yhat[0])
    obs = test[t]
    history.append(obs)
    print('predicted=%f, expected=%f' % (yhat, obs))

pred = np.array(predictions)

error = mean_squared_error(test, predictions)
print('Test MSE: %.3f' % error)

# plot
plt.plot(test)
plt.plot(predictions, color='red')
plt.show()