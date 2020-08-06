
import talib
import pandas as pd

cimport cython

cdef class BaseStrategy:

    cdef:
        readonly long freeze
        readonly unsigned int atr_n, interval, shift
        readonly double atr_scale
        readonly str resample, atr_name, name, stop_loss_name
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

    cdef set_atr_stop_loss(self, int atr_n, double atr_scale):
        """ Funtion to set stop loss using ATR """
        self.enable_stop_loss = True
        self.atr_n     = atr_n
        self.atr_scale = atr_scale
        self.atr_name  = 'ATR|n={atr_n}|interval=1|shift=0'.format(
            atr_n = atr_n
        )
        self.stop_loss_cache = {
            self.atr_name: {
                'func'    : lambda df: pd.DataFrame(talib.ATR(df.high, df.low, df.close, atr_n), columns = [self.atr_name]),
                'interval': 1,
                'shift'   : 0,
            }
        }
        self.stop_loss_name = 'ATR|n={atr_n}|scale={atr_scale}'.format(
            atr_n     = atr_n,
            atr_scale = atr_scale
        )

    cdef get_atr_stop_loss(self, dict snap):
        return -1 * self.atr_scale * snap.get(self.atr_name, 100)


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
            int atr_n = 0, 
            double atr_scale = 0.0):
        super().__init__(interval, shift)
        if atr_scale > 0 and atr_scale > 0:
            self.set_atr_stop_loss(atr_n, atr_scale)
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
                if drawdown < self.get_atr_stop_loss(major_snap):
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
        
        if (self.freeze <= 0) and (to_eod > 3) and (from_sod > 5):
            # update timer
            self.freeze = self.freeze - 1

            # stop loss
            if self.enable_stop_loss and (position != 0): 
                if drawdown < self.get_atr_stop_loss(major_snap):
                    return 0 - position

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
