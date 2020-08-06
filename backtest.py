
import datetime
import talib
import time

import pandas as pd
import numpy  as np

class IntradayEventEngine:

    DEFAULT = dict(
        transaction_cost = 0.0,
    )

    def __init__(self, data):
        self.data = data
        self.portfolio = {}
        self.history = {}
        self.syms = self.data.liquidity.columns.tolist()
        self.syms_mapping = {self.syms[i]: i for i in range(len(self.syms))}

    def to_eod(self, reference):
        """ Function to calculate minutes to EOD """
        output = reference['to_day_eod']
        if reference['is_broken_day']:
            if (reference['to_night_eod'] >= 0) and (reference['to_day_eod'] >= 0):
                output = reference['to_night_eod']
            else:
                output = reference['to_day_eod']
        return output

    def from_sod(self, reference):
        """ Function to calculate minutes from SOD """
        output = reference['from_night_sod']
        if reference['is_broken_day']:
            if (reference['from_night_sod'] >= 0) and (reference['from_day_sod'] >= 0):
                output = reference['from_day_sod']
            else:
                output = reference['from_night_sod']

    def load_strategies(self, strategies):
        """ Load strategies """
        self.strategies = strategies
        self.strategies_names = []
        cache_config = {}
        # request_cache
        for strategy in self.strategies:
            cache_config.update(strategy.request_cache())
            self.strategies_names += [strategy.name]
        self.data.cache_custom(cache_config)

    def update_holding(self, orders, snap):
        """ Function to update holding """
        if self.execute:

            position_change = np.array(orders)
            new_position = self.portfolio['position'] + position_change
            # current_price = np.array([snap[sym]['open'] for sym in self.syms])

            # update pnl at beginning of bar
            self.update_snap(snap = snap, price = 'open')

            # # update cost
            # increase_position = (self.portfolio['position'] * position_change) >= 0
            # new_cost = self.portfolio['cost'] * self.portfolio['position'] + position_change * current_price
            # self.portfolio['cost'][increase_position] = new_cost[increase_position]
            # self.portfolio['cost'][new_position == 0] = 0

            # update position
            self.portfolio['position'] = new_position
            # self.portfolio['position'] = self.portfolio['position'] + position_change
            
            # update min/max price
            self.portfolio['max_price'][new_position == 0] = 0
            self.portfolio['min_price'][new_position == 0] = 1e+4
            
            # update transaction cost
            transaction_cost = np.abs(position_change).sum(axis = 1) * self.config['transaction_cost']
            self.portfolio['value'] = self.portfolio['value'] - transaction_cost

            # update new position
            self.update_snap(snap = snap, price = 'open')

    def update_snap(self, snap, price = 'open'):
        """ Function to update portfolio """
        # current_price = np.array([snap[sym][price] for sym in self.syms])
        # position_return = self.portfolio['position'] * (current_price - self.portfolio['last_price'])
        # position_return[self.portfolio['last_price'] == 0] = 0
        # self.portfolio['value']      = self.portfolio['value'] + position_return.sum(axis = 1)
        # self.portfolio['last_price'] = (self.portfolio['position'] != 0).dot(np.diag(current_price))

        current_price = np.array([snap[sym][price] for sym in self.syms])
        position_return = self.portfolio['position'].dot(np.diag((current_price - self.portfolio['last_price'])))
        # position_return[self.portfolio['last_price'] == 0] = 0
        self.portfolio['value']      = self.portfolio['value'] + position_return.sum(axis = 1)
        self.portfolio['last_price'] = current_price

        self.portfolio['max_price']  = self.portfolio['max_price'].clip(min = current_price)
        self.portfolio['min_price']  = self.portfolio['min_price'].clip(max = current_price)

        # self.
    def record(self, dt):
        """ Function to record portfolio status """
        self.history[self.portfolio['index']] = {
            'value': self.portfolio['value'],
            'position': self.portfolio['position'],
        }

    def run(self, start_date, end_date, config = None):
        """ Function to run backtest """

        print('Running backtest for {n} strategies'.format(
            n = len(self.strategies)
        ))
        self.config = self.DEFAULT.copy()
        self.config.update(config if config is not None else {})

        start_date  = start_date.strftime('%Y-%m-%d')
        end_date    = end_date.strftime('%Y-%m-%d')

        dt_index    = self.data.dt_profile.index[self.data.dt_profile['date'].between(start_date, end_date)]
        start_index = self.data.dt.get_loc(dt_index.min())
        end_index   = self.data.dt.get_loc(dt_index.max())

        _, reference = self.data.snap_cache(index = start_index)
        liquidity   = self.data.liquidity.loc[reference['date']].nlargest(2)
        self.major  = liquidity.index[0]
        self.minor  = liquidity.index[1]
        self.orders_template = [[0] * len(self.syms)] * len(self.strategies_names)
        self.orders = self.orders_template.copy()
        self.execute = False
        self.time   = {
            'snap': 0,
            'execute_orders': 0,
            'strategies': 0,
            'update_snap': 0,
            'record': 0,
            't1': 0,
        }
        self.portfolio = {
            'index'      : None,
            'value'      : np.zeros((len(self.strategies_names))),
            'position'   : np.zeros((len(self.strategies_names), len(self.syms))),
            'last_price' : np.zeros((len(self.syms))),
            'max_price'  : np.zeros((len(self.strategies_names), len(self.syms))),
            'min_price'  : np.zeros((len(self.strategies_names), len(self.syms))),
        }
        # run thru minute bar
        for index in range(start_index, end_index + 1):
            self._next(index)
        
    def _next(self, index):
        """ Function to move to next time stamp """
        # snap cache data
        t = time.time()
        snap, reference = self.data.snap_cache(index = index)
        self.time['snap'] += time.time() - t
        self.portfolio['index'] = index
        if reference['is_night_sod']:
            self.orders = {} 
            self.sod    = index
            liquidity   = self.data.liquidity.loc[reference['date']].nlargest(2)
            self.major  = liquidity.index[0]
            self.minor  = liquidity.index[1]

        

        # execute orders at the beginning of bar
        t = time.time()
        self.update_holding(self.orders, snap)
        self.orders  = self.orders_template.copy()
        self.execute = False
        self.time['execute_orders'] += time.time() - t

        # update portfolio status at the end of bar
        t = time.time()
        self.update_snap(snap = snap, price = 'close')
        self.time['update_snap'] += time.time() - t

        # record portfolio status
        t = time.time()
        self.record(index)
        self.time['record'] += time.time() - t

        # generate new orders at the end of this bar
        if not reference['is_day_eod']:
            snap_major = snap[self.major]
            close      = snap_major.get('close')
            to_eod     = self.to_eod(reference)
            from_sod   = self.from_sod(reference)
            position_data = self.portfolio['position'][:, self.syms.index(self.major)]
            position_mapping = [0.0] * len(self.syms)
            position_mapping[self.syms_mapping[self.major]] = 1.0
            # price_change = (self.portfolio['last_price'] - self.portfolio['cost'])[:, self.syms.index(self.major)]

            drawdown = close - self.portfolio['max_price'][:, self.syms.index(self.major)]
            drawdown_short = self.portfolio['min_price'][:, self.syms.index(self.major)] - close
            drawdown[position_data < 0] = drawdown_short[position_data < 0]
            # print(position_data)
            # print(self.portfolio['max_price'][:, self.syms.index(self.major)])
            # print(self.portfolio['min_price'][:, self.syms.index(self.major)])
            # print(close)
            # print(drawdown)
            # print('===========================')
            t = time.time()
            for i, strategy in enumerate(self.strategies):
                position_change = strategy.next_snap(
                    close      = close,
                    drawdown   = drawdown[i],
                    position   = position_data[i],
                    snap_major = snap_major,
                    # snap_minor = snap_minor,
                    # reference  = reference,
                    to_eod     = to_eod
                    # from_sod   = from_sod
                )
                if position_change != 0:
                    self.orders[i] = [x * position_change for x in position_mapping]
                    self.execute   = True
            self.time['strategies'] += time.time() - t

    def get_history(self):
        """ Function to aggregate history data """
        wealth         = pd.DataFrame.from_dict({k: v['value'] for k, v in self.history.items()}, orient = 'index', columns = self.strategies_names)
        wealth.index   = self.data.dt[wealth.index]
        position       = pd.DataFrame.from_dict({k: v['position'].sum(axis = 1) for k, v in self.history.items()}, orient = 'index', columns = self.strategies_names)
        position.index = self.data.dt[position.index]
        output         = dict(
            wealth   = wealth,
            position = position,
        )
        return output



if __name__ == "__main__":

    import time
    from data.process_data  import FuturesData
    from strategy           import SMA, RSI
    # from backtest           import IntradayEventEngine

    START_DATE = datetime.date(2019, 6, 1)
    END_DATE   = datetime.date(2019, 6, 30)

    # get data
    fd = FuturesData(
        sym        = 'RB',
        start_date = START_DATE,
        end_date   = END_DATE, 
    )

    strategies = [SMA(
        n_fast   = f, 
        n_slow   = 21, 
        atr_n    = 21,
        atr_scale = 3,
        interval = 5, 
        shift    = 0) 
        for f in [3, 5, 8]]
    iee = IntradayEventEngine(fd)
    iee.load_strategies(strategies)
    t = time.time()
    iee.run(
        start_date = START_DATE, 
        end_date = END_DATE,
        config = {'transaction_cost': 0.25}
    )
    print('Running Time: {:.2f}'.format(time.time() - t))
    print('Snap Time: {:.2f}'.format(iee.time['snap']))
    print('Execution Time: {:.2f}'.format(iee.time['execute_orders']))
    print('Strategies Time: {:.2f}'.format(iee.time['strategies']))
    print('Update Snap Time: {:.2f}'.format(iee.time['update_snap']))
    print('Record Time: {:.2f}'.format(iee.time['record']))