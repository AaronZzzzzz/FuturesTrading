
import datetime
import talib
import time

import pandas as pd
import numpy  as np

cimport cython

cdef class IntradayEventEngine:

    cdef:
        readonly dict time, portfolio, history, config
        dict syms_mapping
        double[:, :] orders, orders_template
        bint execute
        long sod, n_strategies, count
        list syms, strategies, strategies_names
        object data
        str major, minor
        long tmp_count

    DEFAULT = dict(
        transaction_cost = 0.0,
    )

    def __init__(self, data):
        self.data = data
        self.portfolio = {}
        self.syms = self.data.liquidity.columns.tolist()
        self.syms_mapping = {self.syms[i]: i for i in range(len(self.syms))}

    cdef to_eod(self, dict reference):
        """ Function to calculate minutes to EOD """
        output = reference['to_day_eod']
        if reference['is_broken_day']:
            if (reference['to_night_eod'] >= 0) and (reference['to_day_eod'] >= 0):
                output = reference['to_night_eod']
            else:
                output = reference['to_day_eod']
        return output

    cdef from_sod(self, dict reference):
        """ Function to calculate minutes from SOD """
        output = reference['from_night_sod']
        if reference['is_broken_day']:
            if (reference['from_night_sod'] >= 0) and (reference['from_day_sod'] >= 0):
                output = reference['from_day_sod']
            else:
                output = reference['from_night_sod']
        return output

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
        self.n_strategies = len(self.strategies)
    
    @cython.boundscheck(False)
    @cython.wraparound(False)
    @cython.nonecheck(False)
    cdef update_holding(self, double[:, :] orders, dict snap):
        """ Function to update holding """
        if self.execute:

            # update pnl at beginning of bar
            self.update_snap(snap = snap, price = 'open')

            # update position
            new_position = self.portfolio['position'] + orders
            self.portfolio['position'] = new_position

            # update min/max price
            self.portfolio['max_price'][new_position == 0] = 0
            self.portfolio['min_price'][new_position == 0] = 1e+4
            
            # update transaction cost
            transaction_cost = np.abs(orders).sum(axis = 1) * self.config['transaction_cost']
            self.portfolio['value'] = self.portfolio['value'] - transaction_cost

            # update new position
            self.update_snap(snap = snap, price = 'open')
    
    @cython.boundscheck(False)
    @cython.wraparound(False)
    @cython.nonecheck(False)
    cdef update_snap(self, dict snap, str price = 'open'):
        """ Function to update portfolio """
        current_price = np.array([snap[sym].get(price, 0) for sym in self.syms])
        position_return = self.portfolio['position'].dot(np.diag((current_price - self.portfolio['last_price'])))
        position_check  = self.portfolio['position'] == 0
        self.portfolio['value']      = self.portfolio['value'] + position_return.sum(axis = 1)
        self.portfolio['last_price'] = current_price

        self.portfolio['max_price']  = self.portfolio['max_price'].clip(min = np.array([snap[sym].get('high', 0) for sym in self.syms]))
        self.portfolio['max_price'][position_check] = 0
        self.portfolio['min_price']  = self.portfolio['min_price'].clip(max = np.array([snap[sym].get('low', 0) for sym in self.syms]))
        self.portfolio['min_price'][position_check] = 9999

        # self.
    cdef record(self, long dt):
        """ Function to record portfolio status """
        cdef unsigned int i
        self.history['idx'][self.count] = dt
        self.history['wealth'][self.count] = self.portfolio['value']
        for i in range(len(self.syms)):
            self.history[self.syms[i]][self.count] = self.portfolio['position'][:, i]
        self.count += 1

    def run(self, start_date, end_date, config = None):
        """ Function to run backtest """
        cdef:
            unsigned long index, start_index, end_index

        print('Running backtest for {n} strategies'.format(
            n = len(self.strategies)
        ))
        self.data.prepare_snap()
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
        self.orders_template = np.zeros((len(self.strategies_names), len(self.syms)))
        self.orders = self.orders_template.copy()
        self.execute = False
        self.history = {
            'idx': np.zeros((end_index - start_index + 1)),
            'wealth': np.zeros((end_index - start_index + 1, self.n_strategies)),
        }
        self.history.update({
            sym: np.zeros((end_index - start_index + 1, self.n_strategies))
            for sym in self.syms
        })
        self.count = 0
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
        self.tmp_count = 0
        # run thru minute bar
        for index in range(start_index, end_index + 1):
            self._next(index)

    @cython.boundscheck(False)
    @cython.wraparound(False)
    @cython.nonecheck(False)  
    cdef _next(self, long index):
        """ Function to move to next time stamp """
        cdef:
            dict reference, snap_major
            double close
            double[:, :] new_position
            long to_eod, from_sod, position_change 
            long sym_index
            unsigned int i

        # snap cache data
        t = time.time()
        snap, reference = self.data.snap_cache(index = index)
        self.time['snap'] += time.time() - t
        self.portfolio['index'] = index
        
        if reference['is_night_sod']:
            self.sod    = index
            liquidity   = self.data.liquidity.loc[reference['date']].nlargest(2)
            if liquidity.index[0] != self.major:
                print(reference['date'])
                # move position
                # before_sym_idx  = self.syms.index(self.major)
                # after_sym_idx   = self.syms.index(liquidity.index[0])
                # before_position = self.portfolio['position'] + self.orders
                # new_position  = self.portfolio['position'] + self.orders
                # new_position[:, self.syms.index(self.major)] = 0.0
                self.orders = -1 * self.portfolio['position']
                self.execute = True

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

        # generate signals at the end of this bar
        if not reference['is_day_eod']:
            # market and reference
            major_snap     = snap[self.major]
            minor_snap     = snap[self.minor]
            to_eod         = self.to_eod(reference)
            from_sod       = self.from_sod(reference)
            sym_index      = self.syms.index(self.major)

            # position
            position_data  = self.portfolio['position'][:, sym_index]

            # drawdown
            close          = major_snap['close']
            drawdown       = close - self.portfolio['max_price'][:, sym_index]
            drawdown_short = self.portfolio['min_price'][:, sym_index] - close
            drawdown[position_data < 0] = drawdown_short[position_data < 0]

            t = time.time()
            for i in range(self.n_strategies):
                position_change = self.strategies[i].next_snap(
                    major_snap = major_snap,
                    minor_snap = minor_snap,
                    position   = position_data[i],
                    drawdown   = drawdown[i],
                    from_sod   = reference['from_night_sod'],
                    to_eod     = reference['to_day_eod'],
                    is_next_day = reference['is_next_day']
                    # from_sod   = from_sod,
                    # to_eod     = to_eod
                    # from_night_sod = reference['from_night_sod']
                )
                if position_change != 0:
                    self.orders[i, sym_index] = position_change
                    self.execute = True
                    self.tmp_count = self.tmp_count + 1
                    # print(self.tmp_count)
                    # print(self.data.dt[index])
                    # print(position_change)
                    # print(drawdown[i])
                    # print(self.portfolio['max_price'])
                    # print(self.portfolio['min_price'])
                    # print(self.strategies[i].benchmark)
                    # print(major_snap)
            self.time['strategies'] += time.time() - t

    def get_history(self):
        """ Function to aggregate history data """
        wealth = pd.DataFrame(
            data    = self.history['wealth'], 
            index   = self.data.dt[self.history['idx'].astype(int)],
            columns = self.strategies_names
        )
        position = pd.DataFrame(
            data    = np.array([self.history[sym] for sym in self.syms]).sum(axis = 0), 
            index   = self.data.dt[self.history['idx'].astype(int)],
            columns = self.strategies_names
        )
        output         = dict(
            wealth   = wealth,
            position = position,
        )
        return output

    def get_history_details(self):
        """ Function to generate detailed history data """
        cdef:
            unsigned int i
            dict output

        output = dict()
        output['dt'] = self.data.dt[self.history['idx'].astype(int)]
        for i in range(len(self.strategies)):
            output[self.strategies[i].name] = {
                'wealth': self.history['wealth'][:, i],
                'position': {sym: self.history[sym][:, i] for sym in self.syms},
            }
        return output


