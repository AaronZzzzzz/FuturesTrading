
import pandas as pd
import numpy  as np

def backtest_analytics(wealth, position):

    total  = pd.DataFrame(wealth.iloc[[-1]].sum(), columns = ['wealth'])
    covers = pd.DataFrame(100 * (position != 0).abs().sum() / len(position), columns = ['covers'])
    trades = pd.DataFrame((position.diff()!=0).abs().sum(), columns = ['trades'])
    res    = pd.concat([total, covers, trades], axis = 1)
    res['pnl_per_trade'] = (res['wealth']) / res['trades']
    res['pnl_per_min'] = (res['wealth']) / res['covers']
    res['std'] = wealth.diff().std()
    res['max_drawdown'] = wealth.apply(lambda x: np.max(np.maximum.accumulate(x) - x), axis = 0)
    res['sharpe'] = res['pnl_per_min'] / res['std']
    res['calmar'] = res['wealth'] / res['max_drawdown']
    res['avg_holding'] = res['covers'] * len(position) / 100.0 / res['trades']
    res = res.sort_values('wealth')
    return res


# def generate_strategy_details(data, dt, detail):
#     """ Function to generate strategy details """

    
#     position = {sym: 0 for sym in detail['position'].keys()}
#     n_syms   = len(position)
#     for i in range(len(dt)):
#         for j in range(n_syms):


