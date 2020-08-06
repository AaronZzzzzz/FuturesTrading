

import datetime
import itertools
import pandas as pd
import numpy  as np

from data.process_data  import FuturesData
from strategy           import SMA, RSI
from optimizer          import PortfolioOptimizer 
from kplot              import kplot

START_DATE = datetime.date(2019, 6, 1)
END_DATE   = datetime.date(2019, 10, 30)

# get data
fd = FuturesData(
    sym        = 'RB',
    start_date = START_DATE,
    end_date   = END_DATE, 
)

# generate and optimize strategies
# generator_config = {
#     'SMA': {
#         'mod': SMA,
#         'args': {
#             'n_periods': [3, 5, 8, 13, 21],
#             'interval' : [1, 3, 5, 8, 13, 15],
#             'shift'    : [0, 1, 2, 3, 5, 8],
#         }
#     },
#     'RSI': {
#         'mod': RSI,
#         'args': {
#             'n_periods': [9, 13, 21],
#             'on_threshold': [20, 25, 30, 35, 40],
#             'off_threshold': [10, 15, 20, 25, 30],
#             'interval' : [1, 3, 5, 8, 13, 15],
#             'shift'    : [0, 1, 2, 3, 5, 8],
#         }
#     },
# }

generator_config = {
    'SMA': {
        'mod': SMA,
        'args': {
            'n_fast': [3, 5, 8, 13, 21],
            'n_slow': []
            'interval' : [1, 3, 5, 8, 13, 15],
            'shift'    : [0, 1, 2, 3, 5, 8],
        }
    },
}

# generate strategies
strategies = []
for v in generator_config.values():
    mod = v['mod']
    args_name = list(v['args'].keys())
    args_value = list(itertools.product(*[v['args'][k] for k in args_name]))
    args = [dict(zip(args_name, x)) for x in args_value]
    strategies += [mod(**x) for x in args if x['interval'] > x['shift']]

# run backtest
iee = IntradayEventEngine(fd)
iee.load_strategies(strategies)
t = time.time()
iee.run(
    start_date = START_DATE, 
    end_date = END_DATE,
    config = {'transaction_cost': 0.25}
)
print('Running Time: {:.2f}'.format(time.time() - t))
        
# run optimization
po = PortfolioOptimizer(history['wealth'])
opt_portfolio = po.solve(30)

# plot result
wealth = iee.get_history()[opt_portfolio.index].sum(axis = 1) - 100000



from kplot import kplot
bkt_data = fd.get_market_data(START_DATE, END_DATE)
kplot(bkt_data)










sg            = StrategyGenerator(fd)
strategies    = sg.generate(generator_config, START_DATE, END_DATE)


# filter strategies
details = pd.DataFrame({k: v['details'] for k, v in strategies.items()}).T
details = details.loc[details.signal_count > 100]
filtered_strats = {k: strategies[k] for k in details.index}
strats_return = pd.concat([pd.Series(v['wealth'].diff().fillna(0), name = k) for k, v in filtered_strats.items()], axis = 1)
strats_return = strats_return.loc[strats_return.index < '2019-08-30']
strats_std    = strats_return.rolling(20).std().mean().sort_values()
strats_std    = strats_std.loc[strats_std < 1.5]
filtered_strats = {k: strategies[k] for k in strats_std.index}


po            = PortfolioOptimizer(filtered_strats)
opt_portfolio = po.solve(
    n_strategies  = 20,
    return_lambda = 0.5,
    start_date    = None,
    end_date      = datetime.date(2019, 8, 30)
)


# plot result
bkt_data = fd.get_min_bar(
    interval   = 1,
    shift      = 0,
    trim       = False,
    start_date = START_DATE,
    end_date   = END_DATE,
)
wealth = None
for s in opt_portfolio.index:
    wealth = strategies[s]['wealth'] if wealth is None else wealth + strategies[s]['wealth'] 
kplot(bkt_data, wealth = wealth)


kplot(bkt_data, wealth = filtered_strats[opt_portfolio.index[7]]['wealth'])


