
import talib
import pandas as pd

from template cimport BaseStrategy 

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

