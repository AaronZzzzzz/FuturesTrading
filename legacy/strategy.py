
import talib
import datetime
import pandas as pd

from data.process_data import FuturesData
from backtest          import IntradayBacktest

class BaseStrategy:

    def __init__(self, data):
        self.data = data

    def symmetrical_cross_signal(self, 
        signal_line, 
        ref_line, 
        threshold = 0, 
        reverse   = False,
        overnight = True):
        """ Function to generate cross signal """

        # trade side
        UP_SIDE   = ['buy', 'sell'] if not reverse else ['short', 'cover']
        DOWN_SIDE = ['short', 'cover'] if not reverse else ['buy', 'sell']

        # cross up signal
        up_position = ((signal_line - ref_line).fillna(0) > threshold).astype(float).shift(1).fillna(0)
        up_position[-1] = 0
        if not overnight:
            up_position[up_position.index.time == datetime.time(15, 0)] = 0
        up_signal = up_position.diff().fillna(0)
        up_signal = up_signal[up_signal != 0]
        up_signal[up_signal.index[up_signal == 1]] = UP_SIDE[0]
        up_signal[up_signal.index[up_signal == -1]] = UP_SIDE[1]
        up_signal = up_signal.dropna()

        # cross down signal
        down_position = ((ref_line - signal_line).fillna(0) > threshold).astype(float).shift(1).fillna(0)
        down_position[-1] = 0
        if not overnight:
            down_position[down_position.index.time == datetime.time(15, 0)] = 0
        down_signal = down_position.diff().fillna(0)
        down_signal = down_signal[down_signal != 0]
        down_signal[down_signal.index[down_signal == 1]] = DOWN_SIDE[0]
        down_signal[down_signal.index[down_signal == -1]] = DOWN_SIDE[1]
        down_signal = down_signal.dropna()

        # combine signal
        signal = pd.concat([up_signal, down_signal]).sort_index()
        return signal

    def asymmetrical_cross_signal(self, 
        signal_line, 
        ref_line, 
        on_threshold  = 0, 
        off_threshold = 0,
        reverse       = False,
        overnight     = True):
        """ Function to generate cross signal """

        # trade side
        UP_SIDE   = ['buy', 'sell'] if not reverse else ['short', 'cover']
        DOWN_SIDE = ['short', 'cover'] if not reverse else ['buy', 'sell']

        # cross up signal
        up_position_on  = ((signal_line - ref_line).fillna(0) > on_threshold).astype(float)
        up_position_on  = up_position_on.where(up_position_on > 0)
        up_position_off = ((signal_line - ref_line).fillna(0) < off_threshold).astype(float)
        up_position_off = (up_position_off - 1).where(up_position_off > 0)
        up_position     = up_position_on.fillna(up_position_off).shift(1).ffill().fillna(0)
        up_position[-1] = 0
        if not overnight:
            up_position[up_position.index.time == datetime.time(15, 0)] = 0
        up_signal = up_position.diff().fillna(0)
        up_signal = up_signal[up_signal != 0]
        up_signal[up_signal.index[up_signal == 1]] = UP_SIDE[0]
        up_signal[up_signal.index[up_signal == -1]] = UP_SIDE[1]
        up_signal = up_signal.dropna()

        # cross down signal
        down_position_on  = ((ref_line - signal_line).fillna(0) > on_threshold).astype(float)
        down_position_on  = down_position_on.where(down_position_on > 0)
        down_position_off = ((ref_line - signal_line).fillna(0) < off_threshold).astype(float)
        down_position_off = (down_position_off - 1).where(down_position_off > 0)
        down_position     = down_position_on.fillna(down_position_off).shift(1).ffill().fillna(0)
        down_position[-1] = 0
        if not overnight:
            down_position[down_position.index.time == datetime.time(15, 0)] = 0
        down_signal = down_position.diff().fillna(0)
        down_signal = down_signal[down_signal != 0]
        down_signal[down_signal.index[down_signal == 1]] = DOWN_SIDE[0]
        down_signal[down_signal.index[down_signal == -1]] = DOWN_SIDE[1]
        down_signal = down_signal.dropna()

        # combine signal
        signal = pd.concat([up_signal, down_signal]).sort_index()
        return signal


class SMA(BaseStrategy):

    def __init__(self, data):
        super().__init__(data)

    def generate_signal(self, 
        n_periods,
        start_date,
        end_date, 
        interval = 1,
        shift    = 0,
        df       = None):
        """ Function to generate signal """
        # get data
        if df is None:
            df = self.data.get_min_bar(
                interval = interval, 
                shift = shift, 
                start_date = start_date, 
                end_date = end_date,
            )
        # generate signal
        signal = self.symmetrical_cross_signal(
            signal_line = df.close, 
            ref_line    = talib.SMA(df.close, n_periods), 
            threshold   = 0,
            overnight   = False)
        return signal


class RSI(BaseStrategy):

    def __init__(self, data):
        super().__init__(data)

    def generate_signal(self,
        n_periods,
        on_threshold,
        off_threshold,
        start_date,
        end_date,
        interval = 1,
        shift    = 0,
        df       = None):
        """ Function to generate signal """
        # get data
        if df is None:
            df = self.data.get_min_bar(
                interval = interval, 
                shift = shift, 
                start_date = start_date, 
                end_date = end_date,
            )
        # generate signal
        benchmark = pd.Series(50, index = df.index)
        signal = self.asymmetrical_cross_signal(
            signal_line   = talib.RSI(df.close, n_periods),
            ref_line      = benchmark,
            on_threshold  = on_threshold,
            off_threshold = off_threshold,
            reverse       = True,
            overnight     = False)
        return signal


if __name__ == "__main__":
    fd = FuturesData(
        sym = 'RB',
        start_date = datetime.date(2019, 3, 1),
        end_date = datetime.date(2019, 3, 31), 
    )
    sma = SMA(fd)

    start_date = datetime.date(2019, 3, 1)
    end_date = datetime.date(2019, 3, 31)

    ib  = IntradayBacktest(fd)
    wealth1, position, bkt_data, _ = ib.run_vectorize(
        signal = sma.generate_signal(
            n_periods = 11,
            start_date = start_date,
            end_date = end_date, 
            interval = 7,
            shift    = 0
        ), 
        start_date = start_date,
        end_date = end_date
    )
    print(wealth1[-1])

    wealth2, position, bkt_data, _ = ib.run_vectorize(
        signal = sma.generate_signal(
            n_periods = 8,
            start_date = start_date,
            end_date = end_date, 
            interval = 21,
            shift    = 3
        ), 
        start_date = start_date,
        end_date = end_date
    )
    print(wealth2[-1])


    from kplot import kplot
    kplot(bkt_data, wealth = wealth1+wealth2)
    print(wealth1[-1])
