
import talib
import pandas as pd

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
