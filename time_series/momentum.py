

import pandas as pd
import numpy  as np
import statsmodels.api as sm
import datetime
import matplotlib.pyplot as plt
from data.process_data import FuturesData

# backtest range
DATA_DATE  = datetime.date(2019, 12, 15)
START_DATE = datetime.date(2020, 1, 1)
# END_DATE   = datetime.date(2019, 2, 11)
END_DATE   = datetime.date(2020, 9, 15)

# get data
fd = FuturesData(
    sym        = 'RB',
    start_date = DATA_DATE,
    end_date   = END_DATE, 
)


def max_drawdown(high, low):
    return np.max(np.maximum.accumulate(high) - low)

def generate_target(price_series, min_period = 5):
    base_value = price_series.open[0]
    target_value = np.zeros((len(price_series),))
    accum_high   = price_series.high[:min_period].max()
    accum_low    = price_series.low[:min_period].min()
    mdd_positive = 0
    mdd_negative = 0
    for i in range(min_period, len(price_series)):
        movement        = price_series.close[i-1] - base_value
        sign            = np.sign(movement)
        accum_high      = max(accum_high, price_series.high[i-1])
        accum_low       = min(accum_low, price_series.low[i-1])
        mdd_positive    = max(mdd_positive, accum_high - price_series.low[i-1])
        mdd_negative    = max(mdd_negative, price_series.high[i-1] - accum_low)
        this_mdd        = mdd_positive if sign >= 0 else mdd_negative
        # this_series     = price_series[:i+1]
        # this_mdd        = max_drawdown(this_series.high, this_series.low) if sign >= 0 else max_drawdown(-1 * this_series.low, -1 * this_series.high)
        target_value[i] = movement / max(1, this_mdd)
    return target_value[np.argmax(np.abs(target_value))]
        


market_data = fd.get_market_data(start_date=datetime.date(2020, 2, 15), end_date=datetime.date(2020, 9, 15), adjust = True, adjust_method = 'backward')
raw_data = fd.get_market_data(start_date=datetime.date(2020, 2, 15), end_date=datetime.date(2020, 9, 15), adjust = False)
resample_5m = fd._resample(market_data, interval = 5)
resample_raw = fd._resample(raw_data, interval = 5)


length = 48
target_value = pd.Series(index = resample_5m.index)
for i in range(length, len(target_value)):
    target_value[i] = generate_target(resample_5m[i-length:i], min_period = 4)
    print(i)



resample_5m['return'] = resample_5m['close'].pct_change()
resample_5m['return_sq'] = resample_5m['close'].pct_change().pow(0.5)
resample_5m['return_cb'] = resample_5m['close'].pct_change().pow(0.3)
resample_5m['1h_return'] = resample_5m['return'].rolling(48).mean()
resample_5m['1h_std'] = resample_5m['return'].rolling(48).std()
resample_5m['1h_zscore'] = resample_5m['1h_return'] / resample_5m['1h_std']



resample_5m['1h_return_sq'] = resample_5m['1h_return'].pow(0.5)
resample_5m['1h_return_cb'] = resample_5m['1h_return'].pow(0.3)
resample_5m['4h_return'] = resample_5m['return'].rolling(48 * 5).sum()
# resample_5m['forward_return'] = resample_5m['return'].shift(-1)
resample_5m = resample_5m.fillna(0)

y = target_value.shift(-(length + 1)).fillna(0)
X = sm.add_constant(resample_5m[[
    'return', 
    'return_sq', 'return_cb', 
    '1h_return', '1h_std', '1h_zscore',
    '1h_return_sq', '1h_return_cb', 
    '4h_return'
]], prepend = False)
mod = sm.OLS(y[:2000], X[:2000])
res = mod.fit()
res.summary()



X_predict = X[2000:]
y_predict = res.predict(X_predict)
y_actual  = resample_5m['return'][2000:]
wealth = pd.Series(1.0, index = X_predict.index)
pos_exit = 0
sign = 0
for i in range(1, len(wealth)):

    if i > pos_exit:
        pos_exit = 0
        wealth[i] = wealth[i-1]
    else:
        wealth[i] = wealth[i-1] * (1 + sign * y_actual[i-1])

    if y_predict[i] > 1:
        pos_exit = i + length
        sign = 1
    elif y_predict[i] < -1:
        pos_exit = i + length
        sign = -1


wealth = np.cumprod(1 + np.sign(res.predict(X[2000:])) * y[2000:])
wealth
plt.plot(wealth)
plt.show()










rolling_period = 24
lag =  6
threshold = resample_5m.high.rolling(rolling_period).max().shift(lag).fillna(99999)
fast_ma = resample_raw.close.ewm(span = 24).mean()
mid_ma  = resample_raw.close.ewm(span = 120).mean()
# slow_ma = resample_raw.close.ewm(span = 240).mean()
signal = ((resample_5m.close > threshold) & (fast_ma > mid_ma)).shift(1)


num_period = len(resample_5m)
position = 0
position_cost = 0
pause = 0
current_wealth = 0
stop_loss = 20
stop_loss_delay = 12

max_pnl = 0

wealth_series = pd.Series(0, index = resample_5m.index)
position_series = pd.Series(0, index = resample_5m.index)

total_trades = 0
open_time_stamp = []
close_time_stamp = []
close_pnl = []
for i in range(num_period):
    
    # update pnl at bar open
    current_snap = resample_5m.iloc[i]
    floating_pnl = position * (current_snap.open - position_cost)
    max_pnl = max(max_pnl, floating_pnl)

    # pause
    if pause > 0:
        pause -= 1
    
    # check stop loss
    # if (max_pnl - floating_pnl) >= (rstd.iloc[i] * 3):
    if (max_pnl - floating_pnl) >= stop_loss:
        position = 0
        current_wealth += floating_pnl

        pause += stop_loss_delay
        close_time_stamp += [current_snap.name]
        close_pnl += [floating_pnl]
        max_pnl = 0
        floating_pnl = 0
        
    # trigger signal
    if pause <= 0:
        if (signal[i] > 0) and (position == 0):
            position = 1
            position_cost = current_snap.open
            total_trades += 1
            open_time_stamp += [current_snap.name]
        elif signal[i] < 0 and (position == 0):
            position = -1
            position_cost = current_snap.open
            total_trades += 1
    
    # update position
    floating_pnl = position * (current_snap.close - position_cost)
    max_pnl = max(max_pnl, position * (current_snap.high - position_cost))
    wealth_series.iloc[i] = current_wealth + floating_pnl
    position_series.iloc[i] = position

plt.plot(wealth_series)
print(max_drawdown(wealth_series, wealth_series))

trades = pd.DataFrame([open_time_stamp, close_time_stamp, close_pnl]).T
trades = trades.rename(columns = {0: 'start', 1: 'end', 2: 'pnl'})
trades['start_price'] = resample_raw.loc[trades.start].open.values
trades['end_price']   = resample_raw.loc[trades.end].open.values
trades



trades[['start', 'end']] = trades[['start', 'end']] - datetime.timedelta(minutes = 5)

    
from time_series.shinny_connector import to_shinny_xml

to_shinny_xml('C:/Users/Administrator/Desktop/aaa.xml', 'C:/Users/Administrator/Desktop/trades.xml', trades)

