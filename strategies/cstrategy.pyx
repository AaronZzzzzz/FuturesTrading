
import talib
import pandas as pd

cimport cython

cdef class BaseStrategy:

    cdef:
        readonly long freeze
        readonly unsigned int interval, shift
        readonly double stop_loss_atr_scale, fix_stop_loss
        readonly str resample, stop_loss_atr_name, name, stop_loss_name
        readonly dict stop_loss_cache
        readonly bint enable_stop_loss

    def __init__(self, int interval, int shift):
        self.freeze   = 0
        self.interval = interval
        self.shift    = shift
        self.stop_loss_cache = {}
        self.enable_stop_loss = False
        self.resample   = 'interval={interval}|shift={shift}'.format(
            interval = self.interval,
            shift    = self.shift
        )
        self.fix_stop_loss = -10000 

    cdef set_fix_stop_loss(self, double scale):
        """ Funtion to set stop loss using fix points """
        self.enable_stop_loss = True
        self.fix_stop_loss    = -1 * scale
        self.stop_loss_cache  = {}
        self.stop_loss_name   = 'FIX|loss={}'.format(scale)

    cdef get_fix_stop_loss(self):
        return self.fix_stop_loss
        
    cdef set_atr_stop_loss(self, int atr_n, double atr_scale):
        """ Funtion to set stop loss using ATR """
        self.enable_stop_loss = True
        self.stop_loss_atr_scale        = -1 * atr_scale
        self.stop_loss_atr_name  = 'ATR|n={atr_n}|interval=1|shift=0'.format(
            atr_n = atr_n
        )
        self.stop_loss_cache = {
            self.stop_loss_atr_name: {
                'func'    : lambda df: pd.DataFrame(talib.ATR(df.high, df.low, df.close, atr_n), columns = [self.stop_loss_atr_name]),
                'interval': 1,
                'shift'   : 0,
            }
        }
        self.stop_loss_name = 'ATR|n={atr_n}|scale={atr_scale}'.format(
            atr_n     = atr_n,
            atr_scale = atr_scale
        )

    cdef get_atr_stop_loss(self, dict snap):
        return self.stop_loss_atr_scale * snap.get(self.stop_loss_atr_name, 100)


cdef class SMA( BaseStrategy ):

    cdef:
        readonly unsigned int n_fast, n_slow
        readonly str fast_name, slow_name

    @staticmethod
    def validate(n_fast, n_slow, interval, shift, **kwargs):
        if (n_fast >= n_slow) or (interval <= shift):
            return False
        return True

    def __init__(self, 
            int n_fast, 
            int n_slow, 
            int interval, 
            int shift,
            double fix_loss = 0.0,
            int atr_n = 0, 
            double atr_scale = 0.0):
        super().__init__(interval, shift)
        # if atr_scale > 0 and atr_scale > 0:
        #     self.set_atr_stop_loss(atr_n, atr_scale)
        if fix_loss > 0:
            self.set_fix_stop_loss(fix_loss)
        self.n_fast    = n_fast
        self.n_slow    = n_slow
        self.fast_name = 'SMA|n={n}|{resample}'.format(
            n        = self.n_fast,
            resample = self.resample
        )
        self.slow_name = 'SMA|n={n}|{resample}'.format(
            n        = self.n_slow,
            resample = self.resample
        )
        self.name      = 'SMA|fast={fast}|slow={slow}|{stop_loss}|{resample}'.format(
            fast      = self.n_fast,
            slow      = self.n_slow,
            stop_loss = self.stop_loss_name,
            resample  = self.resample
        )

    def next_snap(self, 
            dict major_snap,
            dict minor_snap,
            long position,
            double drawdown, 
            long from_sod,
            long to_eod):
        """ Function to iterate a snap """
        cdef:
            long position_change = 0
            double fast_line, slow_line
        
        if (self.freeze <= 0) and (to_eod > 3) and (from_sod > 5):
            # update timer
            self.freeze = self.freeze - 1

            # stop loss
            if self.enable_stop_loss and (position != 0): 
                # if drawdown < self.get_atr_stop_loss(major_snap):
                if drawdown < self.get_fix_stop_loss():
                    return 0 - position

            # generate signal
            if self.slow_name in major_snap:

                close     = major_snap['close']
                fast_line = major_snap[self.fast_name]
                slow_line = major_snap[self.slow_name]
                
                if   close > fast_line > slow_line:
                    position_change = 1 - position
                elif close < fast_line < slow_line:
                    position_change = -1 - position
                else:
                    position_change = 0 - position

        else:
            # close position at end of day
            position_change = 0 - position
        
        return position_change

    def request_cache(self):
        """ Function to generate request for data module """
        # data transformation function
        def cache_func(n, name):
            def func(df):
                return pd.DataFrame(talib.EMA(df.close, n), columns = [name])
            return func
        # build module
        output = {
            self.fast_name: {
                'func'    : cache_func(self.n_fast, self.fast_name),
                'interval': self.interval,
                'shift'   : self.shift,
            },
            self.slow_name: {
                'func'    : cache_func(self.n_slow, self.slow_name),
                'interval': self.interval,
                'shift'   : self.shift,
            }
        }
        output.update(self.stop_loss_cache)
        return output


cdef class RSI( BaseStrategy ):

    cdef:
        unsigned int n_periods
        int on, off
        str rsi_name

    @staticmethod
    def validate(n_periods, on_threshold, off_threshold, interval, shift, **kwargs):
        res = True
        if (interval <= shift) or (on_threshold < off_threshold):
            res = False
        return res

    def __init__(self, 
            int n_periods, 
            int on_threshold, 
            int off_threshold, 
            int interval, 
            int shift,
            int atr_n = 0, 
            double atr_scale = 0.0):
        super().__init__(interval, shift)
        if atr_scale > 0 and atr_scale > 0:
            self.set_atr_stop_loss(atr_n, atr_scale)
        self.n_periods  = n_periods
        self.on         = on_threshold
        self.off        = off_threshold
        self.rsi_name   = 'RSI|n={n}|{resample}'.format(
            n        = self.n_periods,
            resample = self.resample
        )
        self.name       = 'RSI|n={n}|on={on}|off={off}|{stop_loss}|{resample}'.format(
            n         = self.n_periods,
            on        = self.on,
            off       = self.off,
            stop_loss = self.stop_loss_name,
            resample  = self.resample
        )

    def next_snap(self, 
            dict major_snap,
            dict minor_snap,
            long position,
            double drawdown, 
            long from_sod,
            long to_eod):
        """ Function """
        cdef:
            long position_change = 0
            double rsi_line

        # update timer
        self.freeze = self.freeze - 1

        # stop loss
        if self.enable_stop_loss and (position != 0): 
            if drawdown < self.get_atr_stop_loss(major_snap):
                return 0 - position
    
        if (self.freeze <= 0) and (to_eod > 3) and (from_sod > 5):

            # generate signal
            if self.rsi_name in major_snap:
                rsi_line = major_snap[self.rsi_name]
                if   (rsi_line > 50 + self.on) and (position == 0):
                    position_change = -1
                elif (rsi_line < 50 - self.on) and (position == 0):
                    position_change = 1
                elif (rsi_line < 50 + self.off) and (position < 0):
                    position_change = 1
                elif (rsi_line > 50 - self.off) and (position > 0):
                    position_change = -1
        else:
            position_change = 0 - position
        
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
            self.rsi_name: {
                'func'    : cache_func(self.n_periods, self.rsi_name),
                'interval': self.interval,
                'shift'   : self.shift,
            }
        }
        output.update(self.stop_loss_cache)
        return output





cdef class ATR_RSI( BaseStrategy ):

    cdef:
        readonly unsigned int atr_n, atr_ma, rsi_n, rsi_entry
        readonly str atr_name, atr_ma_name, rsi_name

    @staticmethod
    def validate(atr_n, atr_ma, rsi_n, rsi_entry, interval, shift):
        if interval <= shift:
            return False
        return True

    def __init__(self, 
            int atr_n, 
            int atr_ma, 
            int rsi_n,
            int rsi_entry,
            int interval, 
            int shift,
            double fix_loss = 0.0):
        super().__init__(interval, shift)
        if fix_loss > 0:
            self.set_fix_stop_loss(fix_loss)
        self.atr_n     = atr_n
        self.atr_ma    = atr_ma
        self.rsi_n     = rsi_n
        self.rsi_entry = rsi_entry
        self.name      = 'ATR_RSI|atr_n={atr_n}|atr_ma={atr_ma}|rsi_n={rsi_n}|rsi_entry={rsi_entry}|{stop_loss}|{resample}'.format(
            atr_n     = self.atr_n,
            atr_ma    = self.atr_ma,
            rsi_n     = self.rsi_n,
            rsi_entry = self.rsi_entry,
            stop_loss = self.stop_loss_name,
            resample  = self.resample
        )
        self.atr_name = 'ATR|n={n}|{resample}'.format(
            n        = self.atr_n,
            resample = self.resample
        )
        self.atr_ma_name = 'ATRMA|n={n}|ma={ma}|{resample}'.format(
            n        = self.atr_n,
            ma       = self.atr_ma,
            resample = self.resample
        )
        self.rsi_name = 'RSI|n={n}|{resample}'.format(
            n        = self.rsi_n,
            resample = self.resample
        )


    def next_snap(self, 
            dict major_snap,
            dict minor_snap,
            long position,
            double drawdown, 
            long from_sod,
            long to_eod):
        """ Function to iterate a snap """
        cdef:
            long position_change = 0
            double atr_line, atr_ma_line, rsi_line
        
        # update timer
        self.freeze = self.freeze - 1
     
        # stop loss
        if self.enable_stop_loss and (position != 0): 
            if drawdown < self.get_fix_stop_loss():
                return 0 - position
        
        if (self.freeze <= 0): #and (to_eod > 3) and (from_sod > 5):

            # generate signal
            if self.atr_ma_name in major_snap:

                atr_line    = major_snap[self.atr_name]
                atr_ma_line = major_snap[self.atr_ma_name]
                rsi_line    = major_snap[self.rsi_name]

                if atr_line > atr_ma_line:
                    if rsi_line > 50 + self.rsi_entry:
                        position_change = 1 - position
                    elif rsi_line < 50 - self.rsi_entry:
                        position_change = -1 - position
        else:
            # close position at end of day
            position_change = 0 - position
        
        return position_change

    def request_cache(self):
        """ Function to generate request for data module """
        # build module
        output = {
            self.atr_name: {
                'func'    : lambda df: pd.DataFrame(talib.ATR(df.high, df.low, df.close, self.atr_n), columns = [self.atr_name]),
                'interval': self.interval,
                'shift'   : self.shift,
            },
            self.atr_ma_name: {
                'func'    : lambda df: pd.DataFrame(talib.SMA(talib.ATR(df.high, df.low, df.close, self.atr_n), self.atr_ma), columns = [self.atr_ma_name]),
                'interval': self.interval,
                'shift'   : self.shift,
            },
            self.rsi_name: {
                'func'    : lambda df: pd.DataFrame(talib.RSI(df.close, self.rsi_n), columns = [self.rsi_name]),
                'interval': self.interval,
                'shift'   : self.shift,
            }
        }
        output.update(self.stop_loss_cache)
        return output


cdef class DUAL_THRUST( BaseStrategy ):

    cdef:
        readonly unsigned int n_days
        readonly double upper_scale, lower_scale, upper_bound, lower_bound, benchmark
        readonly str range_name

    @staticmethod
    def validate(**kwargs):
        return True

    def __init__(self, 
            int n_days,
            double upper_scale,
            double lower_scale,
            double fix_loss = 0.0):
        super().__init__(1, 0)
        if fix_loss > 0:
            self.set_fix_stop_loss(fix_loss)
        self.n_days      = n_days
        self.upper_scale = upper_scale
        self.lower_scale = lower_scale
        self.benchmark   = -1
        self.name      = 'DUAL_THRUST|n={n}|ub={ub}|lb={lb}|{stop_loss}'.format(
            n         = n_days,
            ub        = upper_scale,
            lb        = lower_scale,
            stop_loss = self.stop_loss_name,
        )
        self.range_name = 'DTRANGE|n={n}'.format(
            n        = n_days,
        )

    def next_snap(self, 
            dict major_snap,
            dict minor_snap,
            long position,
            double drawdown, 
            long from_sod,
            long to_eod,
            bint is_next_day):
        """ Function to iterate a snap """
        cdef:
            long position_change = 0
            double close

        # update timer
        self.freeze = self.freeze - 1

        # stop loss
        if self.enable_stop_loss and (position != 0): 
            if drawdown < self.get_fix_stop_loss():
                return 0 - position

        # update bound
        # if (from_sod == 1) or (self.benchmark < 0):
        if is_next_day or (self.benchmark < 0):
            # if (from_night_sod == 1) or (self.benchmark < 0):
            self.benchmark   = major_snap['open']
            self.upper_bound = self.benchmark + self.upper_scale * major_snap[self.range_name]
            self.lower_bound = self.benchmark - self.lower_scale * major_snap[self.range_name]
        

        if (self.freeze <= 0) and (to_eod > 3):

            # generate signal
            close = major_snap['close']
            if   close > self.upper_bound:
                position_change = 1 - position
            elif close < self.lower_bound:
                position_change = -1 - position
        
        else:
            # close position at end of day
            position_change = 0 - position

        # if position_change != 0:
        #     print(self.benchmark)
        
        return position_change

    def request_cache(self):
        """ Function to generate request for data module """
        # build module
        def range_func(df):
            res = df.rolling(self.n_days).aggregate({
                'close': ['max', 'min'],
                'high': 'max',
                'low': 'min',
            })
            res['R1'] = res[('high', 'max')] - res[('close', 'min')]
            res['R2'] = res[('close', 'max')] - res[('low', 'min')]
            res = res[['R1', 'R2']].max(axis = 1)
            return pd.DataFrame(res, columns = [self.range_name])
        output = {
            self.range_name: {
                'func'    : range_func,
                'daily'   : True
            }
        }
        output.update(self.stop_loss_cache)
        return output

    def vnpy_transform(self):
        """ Function to generate vnpy transform function """
        def transform(daily_close, daily_high, daily_low, **kwargs):
            idx = -int(self.n_days) - 1
            r1  = max(daily_high[idx:-1]) - min(daily_close[idx:-1])
            r2  = max(daily_close[idx:-1]) - min(daily_low[idx:-1])
            r   = max(r1, r2)
            return {self.range_name: r}
        return transform, {'daily_size': int(self.n_days)}



cdef class R_BREAKER( BaseStrategy ):

    cdef:
        readonly double reverse_scale, break_scale, pivot_scale, pivot_shift
        readonly double b_break, b_setup, b_enter, s_break, s_setup, s_enter, last_close
        readonly bint enable_buy, enable_sell
        readonly str dhigh_name, dlow_name, dclose_name

    @staticmethod
    def validate(**kwargs):
        return True

    def __init__(self, 
            double reverse_scale,
            double break_scale,
            double pivot_scale,
            double pivot_shift,
            double fix_loss = 0.0):
        super().__init__(1, 0)
        if fix_loss > 0:
            self.set_fix_stop_loss(fix_loss)
        self.reverse_scale = reverse_scale
        self.break_scale   = break_scale
        self.pivot_scale   = pivot_scale
        self.pivot_shift   = pivot_shift
        self.last_close    = -1
        self.enable_buy    = False
        self.enable_sell   = False
        self.name      = 'R_BREAKER|rev={rev}|break={bre}|scale={scale}|shift={shift}|{stop_loss}'.format(
            rev       = self.reverse_scale,
            bre       = self.break_scale,
            scale     = self.pivot_scale,
            shift     = self.pivot_shift,
            stop_loss = self.stop_loss_name,
        )
        self.dhigh_name  = 'DHIGH'
        self.dlow_name   = 'DLOW'
        self.dclose_name = 'DCLOSE'

    def next_snap(self, 
            dict major_snap,
            dict minor_snap,
            long position,
            double drawdown, 
            long from_sod,
            long to_eod):
        """ Function to iterate a snap """
        cdef:
            long position_change = 0
        
        # update benchmark
        if (from_sod == 1) or (self.last_close < 0):
            dhigh  = major_snap[self.dhigh_name]
            dlow   = major_snap[self.dlow_name]
            dclose = major_snap[self.dclose_name]
            pivot  = (dhigh + dlow + dclose) / 3
            self.last_close  = dclose
            self.enable_buy  = False
            self.enable_sell = False
            self.b_setup = pivot - self.reverse_scale * (dhigh - dlow)
            self.s_setup = pivot + self.reverse_scale * (dhigh - dlow)
            self.b_enter = 2 * self.pivot_scale * pivot - self.pivot_shift * dhigh
            self.s_enter = 2 * self.pivot_scale * pivot - self.pivot_shift * dlow
            self.b_break = dhigh + self.break_scale * (pivot - dlow)
            self.s_break = dlow  - self.break_scale * (dhigh - pivot)
            # print('=======================')
            # print(self.b_break)
            # print(self.s_setup)
            # print(self.s_enter)
            # print(self.b_enter)
            # print(self.b_setup)
            # print(self.s_break)
            
        # update timer
        self.freeze = self.freeze - 1

        # stop loss
        if self.enable_stop_loss and (position != 0): 
            if drawdown < self.get_fix_stop_loss():
                return 0 - position

        # buy trigger
        if not self.enable_buy:
            self.enable_buy = major_snap['high'] >= self.b_setup

        # sell trigger
        if not self.enable_sell:
            self.enable_sell = major_snap['low'] <= self.s_setup

        if (self.freeze <= 0) and (to_eod > 3):

            # break signal
            close = major_snap['close']
            if   close > self.b_break:
                position_change = 1 - position
            elif close < self.s_break:
                position_change = -1 - position

            # reverse signal
            if self.enable_buy:
                if (close > self.b_enter) and (self.last_close < self.b_enter):
                    position_change = 1 - position

            if self.enable_sell:
                if (close < self.s_enter) and (self.last_close > self.s_enter):
                    position_change = -1 - position
        
        else:
            # close position at end of day
            position_change = 0 - position
        
        return position_change

    def request_cache(self):
        """ Function to generate request for data module """
        # build module
        output = {
            self.dhigh_name: {
                'func'    : lambda df: df[['high']].rename(columns = {'high': self.dhigh_name}),
                'daily'   : True
            },
            self.dlow_name: {
                'func'    : lambda df: df[['low']].rename(columns = {'low': self.dlow_name}),
                'daily'   : True
            },
            self.dclose_name: {
                'func'    : lambda df: df[['close']].rename(columns = {'close': self.dclose_name}),
                'daily'   : True
            },
        }
        output.update(self.stop_loss_cache)
        return output

cdef class TEST( BaseStrategy ):

    cdef:
        readonly unsigned int n_days
        readonly double scale, last_close
        readonly str range_name

    @staticmethod
    def validate(**kwargs):
        return True

    def __init__(self, 
            double scale,
            double fix_loss = 0.0):
        super().__init__(1, 0)
        if fix_loss > 0:
            self.set_fix_stop_loss(fix_loss)
        self.scale = scale
        self.last_close
        # self.name      = 'DUAL_THRUST|n={n}|ub={ub}|lb={lb}|{stop_loss}'.format(
        #     n         = n_days,
        #     ub        = upper_scale,
        #     lb        = lower_scale,
        #     stop_loss = self.stop_loss_name,
        # )
        # self.range_name = 'DTRANGE|n={n}'.format(
        #     n        = n_days,
        # )

    def next_snap(self, 
            dict major_snap,
            dict minor_snap,
            long position,
            double drawdown, 
            long from_sod,
            long to_eod,
            bint is_next_day):
        """ Function to iterate a snap """
        cdef:
            long position_change = 0
            double close

        # update timer
        self.freeze = self.freeze - 1

        # stop loss
        if self.enable_stop_loss and (position != 0): 
            if drawdown < self.get_fix_stop_loss():
                return 0 - position

        # # update bound
        # # if (from_sod == 1) or (self.benchmark < 0):
        # if is_next_day or (self.benchmark < 0):
        #     # if (from_night_sod == 1) or (self.benchmark < 0):
        #     self.benchmark   = major_snap['close']
        #     self.upper_bound = self.benchmark + self.upper_scale * major_snap[self.range_name]
        #     self.lower_bound = self.benchmark - self.lower_scale * major_snap[self.range_name]
        
        if (from_sod == 1) or (self.last_close is None):
            self.last_close = major_snap['close']

        if (self.freeze <= 0) and (to_eod > 3):
            if position == 0:
                # generate signal
                close = major_snap['close']
                if   (close - self.last_close) >= 5:
                    position_change = 1 - position
                elif (close - self.last_close) <= -5:
                    position_change = -1 - position
        
        else:
            # close position at end of day
            position_change = 0 - position

        # if position_change != 0:
        #     print(self.benchmark)
        self.last_close = close
        
        return position_change

    def request_cache(self):
        """ Function to generate request for data module """
        # build module
        # def range_func(df):
        #     res = df.rolling(self.n_days).aggregate({
        #         'close': ['max', 'min'],
        #         'high': 'max',
        #         'low': 'min',
        #     })
        #     res['R1'] = res[('high', 'max')] - res[('close', 'min')]
        #     res['R2'] = res[('close', 'max')] - res[('low', 'min')]
        #     res = res[['R1', 'R2']].max(axis = 1)
        #     return pd.DataFrame(res, columns = [self.range_name])
        # output = {
        #     self.range_name: {
        #         'func'    : range_func,
        #         'daily'   : True
        #     }
        # }
        # output.update(self.stop_loss_cache)
        output = {}
        return output

    def vnpy_transform(self):
        """ Function to generate vnpy transform function """
        def transform(daily_close, daily_high, daily_low, **kwargs):
            idx = -int(1) - 1
            r1  = max(daily_high[idx:-1]) - min(daily_close[idx:-1])
            r2  = max(daily_close[idx:-1]) - min(daily_low[idx:-1])
            r   = max(r1, r2)
            return {'abc': r}
        return transform, {'daily_size': int(1)}

