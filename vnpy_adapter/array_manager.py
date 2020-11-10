import numpy  as np
import pandas as pd
import datetime

from vnpy.app.cta_strategy import (
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
)



class ArrayManager(object):
    """
    For:
    1. time series container of bar data
    2. calculating technical indicator value
    """

    def __init__(self, 
            strategies: list,
            # cache_data = None,
            dt_offset: int = 0,):
        """Constructor"""
        self.min_count: int = 0
        self.daily_count: int = 0
        self.min_size: int = min_size
        self.daily_size: int = daily_size
        self.inited: bool = False
        self.dt_offset: int = 0
        self.last_dt = datetime.date(2000, 1, 1)

        self.transforms = [getattr(x, 'vnpy_transform')() for x in strategies]
        self.transform_func = [x[0] for x in self.transforms]
        self.min_size = max([x[1].get('min_size', 0) for x in self.transforms])
        self.daily_size = max([x[1].get('daily_size', 0) for x in self.transforms])

        self.open_array: np.ndarray = np.zeros(min_size)
        self.high_array: np.ndarray = np.zeros(min_size)
        self.low_array: np.ndarray = np.zeros(min_size)
        self.close_array: np.ndarray = np.zeros(min_size)
        self.volume_array: np.ndarray = np.zeros(min_size)
        self.open_interest_array: np.ndarray = np.zeros(min_size)

        self.daily_open_array: np.ndarray = np.zeros(daily_size)
        self.daily_high_array: np.ndarray = np.zeros(daily_size)
        self.daily_low_array: np.ndarray = np.zeros(daily_size)
        self.daily_close_array: np.ndarray = np.zeros(daily_size)
        self.daily_volume_array: np.ndarray = np.zeros(daily_size)
        self.daily_open_interest_array: np.ndarray = np.zeros(daily_size)

    def update_bar(self, bar: BarData) -> None:
        """
        Update new bar data into array manager.
        """
        self.min_count += 1
        self.daily_count += 1
        if not self.inited and (self.min_count >= self.min_size) and (self.daily_count >= self.daily_size):
            self.inited = True

        self.open_array[:-1] = self.open_array[1:]
        self.high_array[:-1] = self.high_array[1:]
        self.low_array[:-1] = self.low_array[1:]
        self.close_array[:-1] = self.close_array[1:]
        self.volume_array[:-1] = self.volume_array[1:]
        self.open_interest_array[:-1] = self.open_interest_array[1:]

        self.open_array[-1] = bar.open_price
        self.high_array[-1] = bar.high_price
        self.low_array[-1] = bar.low_price
        self.close_array[-1] = bar.close_price
        self.volume_array[-1] = bar.volume
        self.open_interest_array[-1] = bar.open_interest

        if self.daily_size > 0:
            if self.last_dt != (bar.datetime.datetime() + datetime.timedelta(minutes = self.dt_offset)).date():
                    
                self.daily_open_array[:-1] = self.daily_open_array[1:]
                self.daily_high_array[:-1] = self.daily_high_array[1:]
                self.daily_low_array[:-1] = self.daily_low_array[1:]
                self.daily_close_array[:-1] = self.daily_close_array[1:]
                self.daily_volume_array[:-1] = self.daily_volume_array[1:]
                self.daily_open_interest_array[:-1] = self.daily_open_interest_array[1:]

                self.daily_open_array[-1] = bar.open_price
                self.daily_high_array[-1] = bar.high_price
                self.daily_low_array[-1] = bar.low_price
                self.daily_close_array[-1] = bar.close_price
                self.daily_volume_array[-1] = bar.volume
                self.daily_open_interest_array[-1] = bar.open_interest
            else:
                self.daily_high_array[-1] = max(self.daily_high_array[-1], bar.high_price)
                self.daily_low_array[-1] = min(self.daily_low_array[-1], bar.low_price)
                self.daily_volume_array[-1] += bar.volume
                self.daily_close_array[-1] = bar.close_price
                self.daily_open_interest_array[-1] = bar.open_interest

    def transform(self):
        transformed = [f(
            open = self.open_array,
            close = self.close_array,
            high = self.high_array,
            low = self.low_array,
            volume = self.volume_array,
            open_interest = self.open_interest_array,
            daily_open = self.daily_open_array,
            daily_close = self.daily_close_array,
            daily_high = self.daily_high_array,
            daily_low = self.daily_low_array,
            daily_volume = self.daily_volume_array,
            daily_open_interest = self.daily_open_interest_array,
        ) for f in self.transform_func]
        res = {
            'open': self.open_array[-1],
            'close': self.close_array[-1],
            'high': self.high_array[-1],
            'low': self.low_array[-1],
            'volume': self.volume_array[-1],
            'open_interest': self.open_interest_array[-1],
        }
        for d in transformed:
            res.update(d)
        return res

