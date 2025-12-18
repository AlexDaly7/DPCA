import pandas as pd
import sqlite3 as sqlite
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

from statsmodels.tsa.seasonal import seasonal_decompose

db = sqlite.connect("database.db")
cur = db.cursor()
#date = pd.read_sql_query("SELECT DATE FROM dublin", db)
#maxtp = pd.read_sql_query("SELECT MEANTP FROM dublin", db)
#maxtp = maxtp.to_numpy()
#date = date.to_numpy()
#print(maxtp)
#plt.plot(maxtp, date)
#plt.show()

plt.style.use('Solarize_Light2')

maxtp = pd.read_sql_query("SELECT * FROM dublin", db)
maxtp["DATE"] = pd.DatetimeIndex(maxtp["DATE"])
maxtp.set_index("DATE")
result = seasonal_decompose(maxtp, 
                            model ='additive')

result.plot()
plt.show()