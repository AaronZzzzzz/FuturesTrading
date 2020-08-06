import pyximport
pyximport.install(reload_support= True)
import time
import datetime
import importlib
import pandas as pd
from data.process_data  import FuturesData
# from cstrategy          import SMA#, RSI
# from cbacktest          import IntradayEventEngine
import cstrategy
import cbacktest
importlib.reload(cstrategy)
importlib.reload(cbacktest)
import cstrategy
import cbacktest


# DATA_DATE  = datetime.date(2019, 5, 15)
# START_DATE = datetime.date(2019, 6, 1)
# END_DATE   = datetime.date(2019, 6, 30)

DATA_DATE  = datetime.date(2019, 12, 15)
START_DATE = datetime.date(2020, 2, 14)
END_DATE   = datetime.date(2020, 3, 15)

#get data
fd = FuturesData(
    sym        = 'RB',
    start_date = DATA_DATE,
    end_date   = END_DATE, 
)

strategies = []


strategies += [cstrategy.SMA(
    n_fast   = f, 
    n_slow   = s, 
    interval = 10, 
    shift    = 0) 
    for f in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 140, 160, 180]
    for s in [30, 40, 50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 240, 280, 320]
    # for sf in [0, 1, 2, 3, 5]
    if cstrategy.SMA.validate(f, s, 1, 0)]


# strategies += [cstrategy.SMA(
#     n_fast   = f, 
#     n_slow   = s, 
#     interval = i, 
#     shift    = 0) 
#     for f in [3, 5, 8, 13, 21, 34, 55]
#     for s in [8, 13, 21, 34, 55, 89, 144]
#     for i in [1, 3, 5, 7, 10, 13, 15, 21]
#     # for sf in [0, 1, 2, 3, 5]
#     if cstrategy.SMA.validate(f, s, i, 0)]

# strategies += [cstrategy.RSI(
#     n_periods     = n, 
#     on_threshold  = on, 
#     off_threshold = off, 
#     interval = i, 
#     shift    = 0) 
#     for n in [7, 14, 21]
#     for on in [20, 30, 40]
#     for off in [-40, -30, -20, -10, 0, 10, 20, 30]
#     for i in [1, 3, 5, 7, 10, 13, 15, 21]
#     # for sf in [0, 1, 2, 3, 5]
#     if cstrategy.RSI.validate(n, on, off, i, 0)]

iee = cbacktest.IntradayEventEngine(fd)
iee.load_strategies(strategies)
t = time.time()
iee.run(
    start_date = START_DATE, 
    end_date = END_DATE,
    config = {'transaction_cost': 0.0}
)
print('Running Time: {:.2f}'.format(time.time() - t))
print('Snap Time: {:.2f}'.format(iee.time['snap']))
print('Execution Time: {:.2f}'.format(iee.time['execute_orders']))
print('Strategies Time: {:.2f}'.format(iee.time['strategies']))
print('Update Snap Time: {:.2f}'.format(iee.time['update_snap']))
print('Record Time: {:.2f}'.format(iee.time['record']))
    

history = iee.get_history()
wealth = pd.DataFrame(history['wealth'].iloc[[-1]].sum(), columns = ['wealth'])
covers = pd.DataFrame(100 * history['position'].abs().sum() / len(history['position']), columns = ['covers'])
trades = pd.DataFrame(history['position'].diff().abs().sum() / 2, columns = ['trades'])

res    = pd.concat([wealth, covers, trades], axis = 1)
res['pnl_per_trade'] = (res['wealth']) / res['trades']
res['pnl_per_min'] = (res['wealth']) / res['covers']
res['std'] = history['wealth'].diff().std()
res['sharpe'] = res['pnl_per_min'] / res['std']
res = res.sort_values('wealth')
res.to_clipboard()