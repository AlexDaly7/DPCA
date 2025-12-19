import pandas as pd
import sqlite3 as sqlite
import matplotlib.pyplot as plt
from scipy import stats

db = sqlite.connect("database.db") # Init objects
cur = db.cursor()

table = pd.read_sql_query("SELECT DATE, MEANTP FROM dublin", db) # Import from database
dates = table["DATE"]

dates = pd.to_datetime(dates) # Turn to datetime object

plt.plot(dates, table["MEANTP"])
plt.grid()
dates = dates.map(pd.Timestamp.timestamp) # Dates must be in unix for comparison

linearResult = stats.linregress(dates, table["MEANTP"])
print("Slope for seconds: " + str(linearResult.slope))
print("Slope for years: " + str(linearResult.slope*31536000)) # Multiplied by the seconds in a year (Date is in unix)
# Other values are inaccurate due to seasonality

plt.title("Mean air temperature for Dublin")
plt.xlabel("Date")
plt.ylabel("Mean Temperature in Celsius")

plt.show()