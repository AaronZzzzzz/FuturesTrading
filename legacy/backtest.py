
import datetime
import pandas as pd
import numpy as np


from kplot import kplot

class IntradayBacktest:

    def __init__(self, data):

        self.data = data

    def run_vectorize(self, signal, start_date = None, end_date = None, slippage = 0):
        """ Function to run vectorized backtest """
        start_date = signal.index.min().date() if start_date is None else start_date
        end_date   = signal.index.max().date() if end_date   is None else end_date
        bkt_data   = self.data.get_min_bar(
            interval   = 1,
            shift      = 0,
            trim       = False,
            start_date = start_date,
            end_date   = end_date,
        )
        bkt_signal = pd.Series(index = bkt_data.index)
        buy_signal_count   = 0
        short_signal_count = 0
        holding_time       = []
        signal_time = None
        for k, v in signal.items():
            if   v.lower() == 'buy':
                bkt_signal[k] = 1
                buy_signal_count += 1
                signal_time = k
            elif v.lower() == 'short':
                bkt_signal[k] = -1
                short_signal_count += 1
                signal_time = k
            elif v.lower() in ['sell', 'cover']:
                bkt_signal[k] = 0
                holding_time += [(k - signal_time).total_seconds() / 60]
        bkt_signal = bkt_signal.ffill().fillna(0)
        bkt_change = bkt_data['open'].diff().shift(-1)
        bkt_change.iloc[-1] = bkt_data.iloc[-1]['close'] - bkt_data.iloc[-1]['open']
        bkt_return = bkt_change * bkt_signal
        bkt_return.loc[signal.index] = bkt_return.loc[signal.index] - slippage 
        bkt_wealth = bkt_return.cumsum()
        bkt_details = {
            'signal_count': buy_signal_count + short_signal_count,
            'buy_count': buy_signal_count,
            'short_count': short_signal_count,
            'average_holding': np.mean(holding_time) if len(holding_time) > 0 else -1,
            'min_holding': np.min(holding_time) if len(holding_time) > 0 else -1,
            'max_holding': np.max(holding_time) if len(holding_time) > 0 else -1,
        }
        return bkt_wealth, bkt_signal, bkt_data, bkt_details

if __name__ == "__main__":
    from data.process_data import FuturesData

    fd = FuturesData('RB')
    sample_data = fd.get_min_bar(
        interval = 5, 
        shift = 0, 
        start_date = datetime.date(2011, 3, 21), 
        end_date = datetime.date(2011, 3, 31),
    )
    signal = pd.Series(index = sample_data.index)
    signal.iloc[25] = 'buy'
    signal.iloc[70] = 'sell'
    signal.iloc[130] = 'short'
    signal.iloc[270] = 'cover'
    signal.iloc[310] = 'buy'
    signal.iloc[390] = 'sell'
    signal = signal.dropna()

    ib = IntradayBacktest(fd)
    wealth, position, df = ib.run_vectorize(signal)

    from kplot import kplot
    kplot(df, position)

            
             


        



