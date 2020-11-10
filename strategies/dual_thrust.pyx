
import talib
import pandas as pd

from template cimport BaseStrategy 

cdef class DUAL_THRUST( BaseStrategy ):

    cdef:
        readonly unsigned int n_days
        readonly double upper_scale, lower_scale, upper_bound, lower_bound, benchmark, range
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
            long to_eod):
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
        if (from_sod == 1) or (self.benchmark < 0):
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
            r1 = max(daily_high[-self.n_days:]) - min(daily_close[-self.n_days:])
            r2 = max(daily_close[-self.n_days:]) - min(daily_low[-self.n_days:])
            r  = max(r1, r2)
            return {self.range_name: r}
        return transform, {'daily_size': self.n_days}