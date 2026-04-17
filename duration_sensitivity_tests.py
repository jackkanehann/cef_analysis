import sqlite3
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, root_mean_squared_error
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.ticker as ticker

conn_CEF = sqlite3.connect('CEF_database.db')
conn_ALT = sqlite3.connect('DATA_database.db')
conn_ETF = sqlite3.connect('ETF_database.db')

##Import explanatory variable series
df1 = pd.read_sql_query("SELECT observation_date, DGS10 FROM treasury_rate_10yr_table", conn_ALT)

##Import subject CEF data, cumlative NAV and discount
df2 = pd.read_sql_query("SELECT DATE, Discount_0, NAV_Cumulative FROM NBH_table", conn_CEF)

##Import MUB data for control group
df3 = pd.read_sql_query("SELECT DATE, NAV_Cumulative FROM MUB_table", conn_ETF)

##Difference every 10th value in a new data frame
##forward term minus current term --> positive = rising rate
df1['10yr_10d_diff'] = df1['DGS10'].diff(periods=10)
df1 = df1.drop(df1.index[:10])

##alter dates to datetime objects
df1['DATE'] = pd.to_datetime(df1['observation_date'], format='%Y-%m-%d')
df2['DATE'] = pd.to_datetime(df2['DATE'], format = '%m/%d/%Y')
df3['DATE'] = pd.to_datetime(df3['DATE'], format = '%m/%d/%Y')


#Create dependent variable columns in each df
#NAV and discount changes are forward looking, independent variables are backward looking for any given row
df2['NBH_NAV_10d_return'] = df2['NAV_Cumulative'].pct_change(periods=-10)*-1
df2['NBH_NAV_30d_return'] = df2['NAV_Cumulative'].pct_change(periods=-30)*-1
df3['MUB_NAV_10d_return'] = df3['NAV_Cumulative'].pct_change(periods=-10)*-1
df3['MUB_NAV_30d_return'] = df3['NAV_Cumulative'].pct_change(periods=-30)*-1

#positive value = discount widening
#negative value = discount tightening
df2['NBH_discount_10d_change'] = df2['Discount_0'].diff(periods=-10)
df2['NBH_discount_30d_change'] = df2['Discount_0'].diff(periods=-30)

#determine appropriate start fate for analysis; review beginning of CEF data
#2002-12-02
print(df2.head(50))
cutoff_date = pd.to_datetime('2002-12-02', format='%Y-%m-%d')

dfs = [df1, df2, df3]
cols_to_drop = ['NAV_Cumulative','observation_date']

#This loop not modifying the dataframes
for df in dfs:
    df = df[df['DATE'] >= cutoff_date]
    df = df.set_index('DATE')
    df = df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

#df_master = df1.merge(df2.drop(columns=cols_to_drop, errors='ignore'), on='DATE', how='inner').merge(df3.drop(columns=cols_to_drop, errors='ignore'), on='DATE', how='inner')
df_master = df1.merge(df2.drop(columns=cols_to_drop, errors='ignore'), on='DATE', how='inner')
df_master = df_master[['DATE', '10yr_10d_diff','NBH_discount_10d_change', 'NBH_NAV_10d_return']]
df_master = df_master.dropna(subset = ['10yr_10d_diff', 'NBH_discount_10d_change', 'NBH_NAV_10d_return'])
print(df_master.head(5))
###
###
### END OF DATA EXTRACTION, PREPARATION, CLEANSING
###
###

# X must be 2D (DataFrame), y can be 1D (Series)
X = df_master[['10yr_10d_diff']] 
y = df_master['NBH_discount_10d_change']
c = df_master['NBH_NAV_10d_return']

model = LinearRegression().fit(X, y)
predictions = model.predict(X)
mse = mean_squared_error(y, predictions)
rmse = root_mean_squared_error(y, predictions)

print(f"Intercept: {model.intercept_}, Slope: {model.coef_[0]}")
print(f"R^2: {model.score(X, y)}")
print(f"Mean Squared Error: {mse}")
print(f"Root Mean Squared Error: {rmse}")

#create scatterplot w/ normalized divergent colormap
norm = colors.TwoSlopeNorm(vmin=df_master['NBH_NAV_10d_return'].values.min(), vcenter=0, vmax=df_master['NBH_NAV_10d_return'].values.max())
ax = df_master.plot.scatter(x='10yr_10d_diff',y='NBH_discount_10d_change', grid=True, c=c, cmap='RdYlGn', norm = norm, alpha=0.5 ,title="10day Discount Change Given previous 10yr rate movement")

#title for colorbar
cbar = ax.collections[0].colorbar
cbar.set_label('NAV Return in Period')

#create/show trendline
z = np.polyfit(df_master['10yr_10d_diff'], df_master['NBH_discount_10d_change'], 1)
p = np.poly1d(z)
ax.plot(df_master['10yr_10d_diff'], p(df_master['10yr_10d_diff']), color='blue', label='Trendline')

#Adjust x-axis increment
ax.xaxis.set_major_locator(ticker.MultipleLocator(0.2))

plt.legend()
plt.show()

"""
BEHOLD THE TASKS COMPLETED BELOW
"""
##Difference every 10th value in a new data frame DONE
##Import subject CEF data, cumlative NAV and discount DONE
##Import MUB data for control group DONE
##MAKE DATE FORMATS CONGRUENT DONE
#Create 10d, 30d forward returns and changes in discount
### NAV_10d_return, NAV_30d_return, discount_10d_change, discount_30d_change
### include NBH_, MUB_, etc prefixes.  remove DisplayTicker columns
##Remove observations earlier than the earliest return date for the youngest subject CEF
