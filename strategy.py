import math
import talib
import pandas as pd
import numpy  as np

class BaseStrategy:
        
    def __init__(self):
        self.freeze = 0

    def stop_loss(self, price_change, position, threshold):
        if price_change * math.copysign(1, position) <= threshold:
            self.freeze = 5
            return True
        return False

class SMA( BaseStrategy ):

    @staticmethod
    def validate(n_fast, n_slow, atr_n, atr_scale, interval, shift):
        res = True
        if (n_fast >= n_slow) or (interval <= shift):
            res = False
        return res

    def __init__(self, n_fast, n_slow, atr_n, atr_scale, interval, shift):
        super().__init__()
        self.n_fast     = n_fast
        self.n_slow     = n_slow
        self.atr_n      = atr_n
        self.atr_scale  = atr_scale
        self.interval   = interval
        self.shift      = shift
        self.resample   = 'interval={interval}|shift={shift}'.format(
            interval = self.interval,
            shift    = self.shift
        )
        self.cache_name = {k: 'SMA|n={n}|{resample}'.format(
            n        = k,
            resample = self.resample
        ) for k in [self.n_fast, self.n_slow]}
        self.atr_name   = 'ATR|n={n}|interval=1|shift=0'.format(
            n = self.atr_n
        )
        self.name       = 'SMA|fast={fast}|slow={slow}|{stop_loss}|{resample}'.format(
            fast     = self.n_fast,
            slow     = self.n_slow,
            stop_loss = '{}|scale={}'.format(self.atr_name, self.atr_scale),
            resample = self.resample
        )

    def next_snap(self, drawdown, close, position, snap_major, to_eod):
        """ Function """
        position_change  = 0
        current_position = position
        
        if (self.freeze <= 0) & (to_eod > 3):
            # update timer
            self.freeze = self.freeze - 1

            # stop loss
            if current_position > 0: 

                atr = snap_major.get(self.atr_name, 50)
                if drawdown < -atr * self.atr_scale:
                # if self.stop_loss(price_change, current_position, -atr * self.atr_scale):
                    return 0 - current_position

            # generate signal
            if self.cache_name[self.n_slow] in snap_major:
                fast_line = snap_major[self.cache_name[self.n_fast]]
                slow_line = snap_major[self.cache_name[self.n_slow]]
                close     = snap_major['close']
                if   close > fast_line > slow_line:
                    position_change = 1 - current_position
                elif close < fast_line < slow_line:
                    position_change = -1 - current_position
                else:
                    position_change = 0 - current_position
        else:
            # close position at end of day
            position_change = 0 - current_position
        
        return position_change

    def request_cache(self):
        """ Function to generate request for data module """
        # data transformation function
        def cache_func(n, name):
            def func(df):
                return pd.DataFrame(talib.SMA(df.close, n), columns = [name])
            return func
        # build module
        output = {
            name: {
                'func'    : cache_func(n, name),
                'interval': self.interval,
                'shift'   : self.shift,
            }
            for n, name in self.cache_name.items()
        }
        output.update({
            self.atr_name: {
                'func'    : lambda df: pd.DataFrame(talib.ATR(df.high, df.low, df.close, self.atr_n), columns = [self.atr_name]),
                'interval': 1,
                'shift'   : 0,
            }
        })
        return output

class RSI( BaseStrategy ):

    @staticmethod
    def validate(n_periods, on_threshold, off_threshold, interval, shift):
        res = True
        if (interval <= shift) or (on_threshold < off_threshold):
            res = False
        return res

    def __init__(self, n_periods, on_threshold, off_threshold, interval, shift):
        super().__init__()
        self.n_periods  = n_periods
        self.on         = on_threshold
        self.off        = off_threshold
        self.interval   = interval
        self.shift      = shift
        self.resample   = 'interval={interval}|shift={shift}'.format(
            interval = self.interval,
            shift    = self.shift
        )
        self.cache_name = 'RSI|n={n}|{resample}'.format(
            n        = self.n_periods,
            resample = self.resample
        )
        self.name       = 'RSI|n={n}|on={on}|off={off}|{resample}'.format(
            n        = self.n_periods,
            on       = self.on,
            off      = self.off,
            resample = self.resample
        )

    def next_snap(self, close, position, snap_major, to_eod, **kwargs):
        """ Function """
        position_change  = 0
        current_position = position

        if to_eod > 3:
            if self.cache_name in snap_major:
                rsi   = snap_major[self.cache_name]
                if   (rsi > 50 + self.on) and (current_position == 0):
                    position_change = -1
                elif (rsi < 50 - self.on) and (current_position == 0):
                    position_change = 1
                elif (rsi < 50 + self.off) and (current_position < 0):
                    position_change = 1
                elif (rsi > 50 - self.off) and (current_position > 0):
                    position_change = -1
        else:
            position_change = 0 - current_position
        
        return position_change

    def request_cache(self):
        """ Function to generate request for data module """
        # data transformation function
        def cache_func(n, name):
            def func(df):
                return pd.DataFrame(talib.RSI(df.close, n), columns = [name])
            return func
        # build module
        output = {
            self.cache_name: {
                'func'    : cache_func(self.n_periods, self.cache_name),
                'interval': self.interval,
                'shift'   : self.shift,
            }
        }
        return output


if __name__ == "__main__":

    import time
    import datetime
    from data.process_data  import FuturesData
    from strategy           import SMA#, RSI
    from backtest           import IntradayEventEngine

    START_DATE = datetime.date(2019, 6, 1)
    END_DATE   = datetime.date(2019, 6, 30)

    # get data
    fd = FuturesData(
        sym        = 'RB',
        start_date = START_DATE + datetime.timedelta(1),
        end_date   = END_DATE, 
    )

    strategies = []

    strategies += [SMA(
        n_fast   = f, 
        n_slow   = s, 
        atr_n    = atrn,
        atr_scale = atrs,
        interval = i, 
        shift    = sf) 
        for f in [3, 5, 8, 13]#, 21, 34]
        for s in [8, 13, 21, 34]#, 55, 89]
        for i in [7, 8, 10, 13]#, 15, 21]
        for sf in [0, 1, 2, 3, 5]#, 8, 10]
        for atrn in [7, 15, 30, 60]
        for atrs in [1, 2, 3, 5, 10]
        if SMA.validate(f, s, atrn, atrs, i, sf)]

    # strategies += [RSI(
    #     n_periods     = n, 
    #     on_threshold  = on, 
    #     off_threshold = off, 
    #     interval = i, 
    #     shift    = sf) 
    #     for n in [7, 9, 14, 21]
    #     for on in [20, 25, 30, 35, 40, 45]
    #     for off in [5, 10, 15, 20, 25, 30, 35, 40]
    #     for i in [5, 7, 8, 9, 10, 13, 15]
    #     for sf in [0, 1, 2, 3, 5]
    #     if RSI.validate(n, on, off, i, sf)]

    # strategies = [SMA(3, 21, 7, 1)]
    iee = IntradayEventEngine(fd)
    iee.load_strategies(strategies)
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


    # a = res[(res.trades > 30) & 
    #     (res.trades < 300) &
    #     (res.pnl_per_min > -2)]
    #     # (res['std'] < 1.8)]


    # from optimizer import PortfolioOptimizer
    # w = history['wealth'][a.index]
    # po = PortfolioOptimizer(w)
    # opt_portfolio = po.solve(10, return_lambda = 1.0)

    # # plot result
    # from kplot              import kplot
    # wplot = history['wealth'][opt_portfolio.index].sum(axis = 1)
    # bkt_data = fd.get_market_data(START_DATE, END_DATE)
    # kplot(bkt_data, wealth = wplot)

