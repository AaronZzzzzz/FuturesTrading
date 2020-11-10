
import talib
import pandas as pd

from template cimport BaseStrategy 

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

