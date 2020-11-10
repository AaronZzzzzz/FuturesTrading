
# cython setup
import pyximport
pyximport.install(reload_support= True)

import time
import datetime

import importlib
import pandas as pd
from data  import FuturesData
# from optimizer          import PortfolioOptimizer 
from utils.utils              import backtest_analytics
from backtester.kplot              import kplot

import strategies.cstrategy
import backtester.cbacktest
# import cdata
importlib.reload(strategies.cstrategy)
importlib.reload(backtester.cbacktest)
# importlib.reload(cdata)
import strategies.cstrategy
import backtester.cbacktest
# import cdata


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

# strategies
strats = []

# strategies += [cstrategy.SMA(
#     n_fast   = f, 
#     n_slow   = s, 
#     atr_n    = 10,
#     atr_scale = 4,
#     interval = 10, 
#     shift    = 0) 
#     for f in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 140, 160, 180]
#     for s in [30, 40, 50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 240, 280, 320]
#     # for sf in [0, 1, 2, 3, 5]
#     if cstrategy.SMA.validate(f, s, 1, 0)]

# strategies += [cstrategy.RSI(
#     n_periods     = n, 
#     on_threshold  = on, 
#     off_threshold = off, 
#     # atr_n = 5,
#     # atr_scale = 2,
#     interval = 10, 
#     shift    = 0) 
#     for n in [7, 14, 21]
#     for on in [20, 30, 40]
#     for off in [-40, -30, -20, -10, 0, 10, 20, 30]
#     # for i in [1, 3, 5, 7, 10, 13, 15, 21]
#     # for sf in [0, 1, 2, 3, 5]
#     if cstrategy.RSI.validate(n, on, off, 5, 0)]

# strategies += [cstrategy.ATR_RSI(
#     atr_n     = atrn, 
#     atr_ma    = atrm, 
#     rsi_n     = rsin,
#     rsi_entry = rsie,
#     fix_loss  = 30, 
#     interval  = i, 
#     shift     = 0) 
#     for atrn in [5, 10, 15, 20, 30]
#     for atrm in [5, 10, 15, 20, 30]
#     for rsin in [5, 10, 15, 20, 30]
#     for rsie in [10, 20, 30, 40]
#     for i    in [3, 5, 10, 15]]

strats += [strategies.cstrategy.DUAL_THRUST(
    n_days = n,
    fix_loss = fl,
    upper_scale = us, # ~0.2
    lower_scale = ls) # ~0.2
    for n in [1, 2, 3, 4, 5, 6, 8, 10]
    for us in [x / 4 for x in range(1, 13)]
    for ls in [x / 4 for x in range(1, 13)]
    # for us in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    # for ls in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    for fl in [20.0, 25.0, 30.0]]

# strats = [strategies.cstrategy.DUAL_THRUST(
#     n_days = 1,
#     fix_loss = 30,
#     upper_scale = 0.5,
#     lower_scale = .75
# )]



# strategies += [cstrategy.R_BREAKER(
#     reverse_scale = rs,
#     break_scale   = bs,
#     pivot_scale   = 1.0,
#     pivot_shift   = 1.0,
#     fix_loss      = 30)
#     for rs in [0.8, 1.0, 1.2]
#     for bs in [1.5, 1.75, 2.0, 2.25, 2.5]]
#     # for psc in [0.95, 1.0, 1.05]
#     # for psh in [0.9, 1.0, 1.1]]

# run backtest
iee = backtester.cbacktest.IntradayEventEngine(fd)
iee.load_strategies(strats)
t = time.time()
iee.run(
    start_date = START_DATE, 
    end_date = END_DATE,
    config = {'transaction_cost': 0.5}
)
print('Running Time: {:.2f}'.format(time.time() - t))
print('Snap Time: {:.2f}'.format(iee.time['snap']))
print('Execution Time: {:.2f}'.format(iee.time['execute_orders']))
print('Strategies Time: {:.2f}'.format(iee.time['strategies']))
print('Update Snap Time: {:.2f}'.format(iee.time['update_snap']))
print('Record Time: {:.2f}'.format(iee.time['record']))
    
history = iee.get_history()
res     = backtest_analytics(history['wealth'], history['position'])

# filtering
filtered_res = res[res.trades > 50]
filtered_res = filtered_res[filtered_res.wealth > min(filtered_res.wealth.mean(), 0)]
filtered_res = filtered_res[filtered_res.sharpe > 2]
# filtered_res = filtered_res[filtered_res]


# plot best one
filtered_res = res.sort_values('calmar')
best_idx = -1
wealth   = history['wealth'][filtered_res.index[best_idx]]
bkt_data = fd.get_market_data(START_DATE, END_DATE)
kplot(bkt_data, wealth = wealth, position = history['position'][filtered_res.index[best_idx]])


# run optimization
rolling_period = 1125
return_data = history['wealth'][filtered_res.index].diff().fillna(0)
return_data = return_data.clip(
    # lower = return_data.rolling(rolling_period).quantile(0.005),
    # upper = return_data.rolling(rolling_period).quantile(0.995)
    lower = -10,
    upper = 10
)
# po = PortfolioOptimizer(return_data)
# opt_portfolio = po.solve(30, return_lambda = 1.0)

# # plot result
# opt_wealth   = pd.DataFrame(history['wealth'][opt_portfolio.index].mean(axis = 1), columns = ['OPT'])
# opt_position = pd.DataFrame(history['position'][opt_portfolio.index].mean(axis = 1), columns = ['OPT'])
# opt_res      = backtest_analytics(opt_wealth, opt_position)
# bkt_data     = fd.get_market_data(START_DATE, END_DATE)
# kplot(bkt_data, wealth = opt_wealth['OPT'])











