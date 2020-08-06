
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
        cache_config = {}
        # request_cache
        for strategy in self.strategies:
            cache_config.update(strategy.request_cache())
            self.init_strategy(
                name    = strategy.name,
                capital = 10000.,
            )
        self.data.cache_custom(cache_config)

    def init_strategy(self, name, capital = 10000.):
        """ Function to initiate backtest status """
        syms = self.data.liquidity.columns.tolist()
        self.portfolio[name] = dict(
            cash    = capital,
            holding = {
                sym: dict(
                    shares = 0,
                    cost   = 0.0,
                    value  = 0.0,
                    pnl    = 0.0,
                ) for sym in syms
            },
            status  = dict(
                holding_pnl = 0.0,
                value       = capital,
            ),
        )

    def update_holding(self, name, sym, shares, snap):
        """ Function to update holding """
        # print(self.portfolio)
        current_holding = self.portfolio[name]['holding'][sym].copy()
        if shares == 0:
            return
        if shares * current_holding['shares'] >= 0:
            # new/add position
            self.portfolio[name]['holding'][sym].update(dict(
                shares = current_holding['shares'] + shares,
                cost   = current_holding['cost'] + abs(shares) * snap[sym]['market']['open']
            ))
            self.portfolio[name]['cash'] -= abs(shares * snap[sym]['market']['open'])
        else:
            if abs(shares) > abs(current_holding['shares']): 
                # reverse position
                closed_cost = current_holding['cost']
                closed_pnl  = np.sign(current_holding['shares']) * (abs(current_holding['shares']) * snap[sym]['market']['open'] - closed_cost)
                self.portfolio[name]['holding'][sym].update(dict(
                    shares = current_holding['shares'] + shares,
                    cost   = abs(current_holding['shares'] + shares) * snap[sym]['market']['open']
                ))
                self.portfolio[name]['cash'] += closed_cost + closed_pnl - abs((current_holding['shares'] + shares) * snap[sym]['market']['open'])
            else:
                # reduce/close position
                closed_cost = current_holding['cost'] * abs(1.0 * shares / current_holding['shares'])
                closed_pnl  = np.sign(current_holding['shares']) * (abs(current_holding['shares']) * snap[sym]['market']['open'] - closed_cost)
                self.portfolio[name]['holding'][sym].update(dict(
                    shares = current_holding['shares'] + shares,
                    cost   = current_holding['cost'] * (1.0 * (current_holding['shares'] + shares) / current_holding['shares'])
                ))
                self.portfolio[name]['cash'] += closed_cost + closed_pnl
        
        # slippage/commission 
        self.portfolio[name]['cash'] -= abs(shares) * self.config['transaction_cost']

        # update market value
        self.update_snap(name, snap, 'open', True)

    def update_snap(self, name, snap, price = 'open', force_update = False):
        """ Function to update portfolio """

        name  = [name] if name is not None else self.portfolio.keys()
        price = {sym: snap[sym]['market'][price] for sym in snap.keys()}
        for n in name:
            holding_pnl = 0
            total_value = 0
            for sym in self.portfolio[n]['holding'].keys():
                # current_profile = self.portfolio[n]['holding'][sym]
                shares = self.portfolio[n]['holding'][sym]['shares']
                cost   = self.portfolio[n]['holding'][sym]['cost']
                if force_update or (shares != 0):
                    current_value = abs(shares) * price[sym]
                    current_pnl   = (current_value - cost) * np.sign(shares)
                    self.portfolio[n]['holding'][sym]['value'] = current_value
                    self.portfolio[n]['holding'][sym]['pnl']   = current_pnl
                    holding_pnl += current_pnl
                    total_value += current_value
            self.portfolio[n]['status']['holding_pnl'] = holding_pnl
            self.portfolio[n]['status']['value'] = self.portfolio[n]['cash'] + total_value

    def record(self, dt):
        # self.history[dt] = {name: value['status']['value'] for name, value in self.portfolio.items()}


        self.history[dt] = {
            name: {
                'wealth': portfolio['status']['value'],
                'position': {sym: v['shares'] for sym, v in portfolio['holding'].items()},
                # 'orders': {sym: self.orders.get(name, {}).get(sym, 0) for sym in portfolio['holding']}
            }   
            for name, portfolio in self.portfolio.items()
        }

    def run(self, start_date, end_date, config = None):
        """ Function to run backtest """

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
        self.orders = {} 
        self.time   = {
            'snap': 0,
            'execute_orders': 0,
            'strategies': 0,
            'update_snap': 0,
            'record': 0,
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
        if reference['is_night_sod']:
            self.orders = {} 
            self.sod    = index
            liquidity   = self.data.liquidity.loc[reference['date']].nlargest(2)
            self.major  = liquidity.index[0]
            self.minor  = liquidity.index[1]

        snap_major = snap[self.major]
        snap_minor = snap[self.minor]

        t = time.time()
        # execute orders at beginning of this bar
        for name, order in self.orders.items():
            self.update_holding(
                name   = name,
                sym    = order['sym'],
                shares = order['shares'],
                snap   = snap
            )
        self.orders = {}
        self.time['execute_orders'] += time.time() - t

        # generate new orders at the end of this bar
        if not reference['is_day_eod']:
            to_eod   = self.to_eod(reference)
            from_sod = self.from_sod(reference)
            for strategy in self.strategies:
                t = time.time()
                position_change = strategy.next_snap(
                    portfolio  = self.portfolio[strategy.name]['holding'][self.major],
                    snap_major = snap_major,
                    snap_minor = snap_minor,
                    reference  = reference,
                    to_eod     = to_eod,
                    from_sod   = from_sod
                )
                if position_change != 0:
                    self.orders.update({
                        strategy.name: {
                            'sym': self.major,
                            'shares': position_change,
                        }
                    })
                self.time['strategies'] += time.time() - t
            t = time.time()
            self.update_snap(None, snap, 'close')
            self.time['update_snap'] += time.time() - t

        # record history
        t = time.time()
        self.record(index)
        self.time['record'] += time.time() - t

    def get_history(self):
        wealth         = pd.DataFrame({dt: {name: v['wealth'] for name, v in self.history[dt].items()} for dt in self.history}).T
        wealth.index   = self.data.dt[wealth.index]
        position       = pd.DataFrame({dt: {name: sum(v['position'].values()) for name, v in self.history[dt].items()} for dt in self.history}).T
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

    START_DATE = datetime.date(2019, 6, 1)
    END_DATE   = datetime.date(2019, 8, 30)

    # get data
    fd = FuturesData(
        sym        = 'RB',
        start_date = START_DATE,
        end_date   = END_DATE, 
    )


    strategies = [RSI(n, 30, 20, 7, 2) for n in range(3, 23)]
    iee = IntradayEventEngine(fd)
    iee.load_strategies(strategies)
    t = time.time()
    iee.run(
        start_date = datetime.date(2019, 6, 1), 
        end_date = datetime.date(2019, 8, 30),
        config = {'transaction_cost': 0.25}
    )
    print('Running Time: {:.2f}'.format(time.time() - t))
        