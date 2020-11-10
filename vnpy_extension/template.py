
import datetime
import numpy as np
from datetime import time
from data.process_data  import FuturesData
from vnpy_extension.array_manager import ArrayManager
from vnpy_extension.config import CACHE_PATH
from vnpy.app.cta_strategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    # ArrayManager,
)


class CustomTemplate(CtaTemplate):
    """"""

    author = "Aaron Zhang"

    sod = 21 * 60
    eod = 15 * 60

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        
        self.set_dt_profile(vt_symbol)

        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager(
            strategies = self.strategies,
            dt_offset = self.dt_offset
        )
        self.bars = []
        self.position_array: np.ndarray = np.zeros(len(self.strategies))
        self.trade_high_array: np.ndarray = np.zeros(len(self.strategies))
        self.trade_low_array: np.ndarray = np.ones(len(self.strategies)) * 9999
        self.drawdown_array: np.ndarray = np.zeros(len(self.strategies))

        self.count = 0

    def set_dt_profile(self, vt_symbol):
        if vt_symbol[:2].upper() == 'RB':
            self.sod = 21 * 60
            self.eod = 15 * 60
            self.dt_offset = 3 * 60

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.write_log('Initiating strategy')
        self.load_bar(1)

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("Running startegy")

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.write_log("Stopping strategy")

    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        self.bg.update_tick(tick)

    def from_sod(self, bar):
        """ Calculate minutes from sod """
        current_time = bar.datetime.time().hour * 60 + bar.datetime.time().minute
        if current_time >=  self.sod:
            mins = current_time - self.sod
        else:
            mins = current_time + self.dt_offset
        # mins = bar.datetime.time().hour  * 60 + bar.datetime.time().minute - self.sod
        return mins + 1

    def to_eod(self, bar):
        """ Calculate minutes to sod """
        current_time = bar.datetime.time().hour * 60 + bar.datetime.time().minute
        if current_time > self.eod:
            mins = 1440 - current_time + self.eod
        else:
            mins = self.eod - current_time
        return mins - 1

    def auto_order(self, price, order_size):
        if order_size != 0:
            if order_size > 0:
                if self.pos >= 0:
                    self.buy(price * 1.05, abs(order_size), stop = False)
                    self.write_log('Position Increased: {}'.format(str(abs(order_size))))
                else:
                    if (self.pos + order_size) <= 0:
                        self.cover(price * 1.05, abs(order_size), stop = False)
                        self.write_log('Position Decreased: {}'.format(str(abs(order_size))))
                    else:
                        self.cover(price * 1.05, abs(self.pos), stop = False)
                        self.buy(price * 1.05, abs(order_size) - abs(self.pos), stop = False)
                        self.write_log('Position Revered: {}'.format(str(abs(order_size) - abs(self.pos))))
            else:
                if self.pos <= 0:
                    self.short(price * 0.95, abs(order_size), stop = False)
                else:
                    if (self.pos + order_size) >= 0:
                        self.sell(price * 0.95, abs(order_size), stop = False)
                        self.write_log('Position Decreased: {}'.format(str(abs(order_size))))
                    else:
                        self.sell(price * 0.95, abs(self.pos), stop = False)
                        self.short(price * 0.95, abs(order_size) - abs(self.pos), stop = False)
                        self.write_log('Position Revered: {}'.format(str(abs(order_size) - abs(self.pos))))


    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        self.cancel_all()

        am = self.am
        is_next_day = am.is_next_day()
        am.update_bar(bar)
        if not am.inited:
            return

        major_snap = am.transform()
        from_sod   = self.from_sod(bar)
        to_eod     = self.to_eod(bar)

        # if bar.datetime.date() == datetime.date(2019, 2, 26):
        #     print(major_snap)
        #     print(am.daily_close_array[-2])
        #     print(am.daily_high_array[-2])
        #     print(am.daily_low_array[-2])

        # update drawdown
        position_check = self.position_array == 0
        self.trade_high_array = np.maximum(self.trade_high_array, bar.high_price)
        self.trade_high_array[position_check] = 0
        self.trade_low_array = np.minimum(self.trade_low_array, bar.low_price) 
        self.trade_low_array[position_check] = 9999
        self.drawdown_array[self.position_array > 0] = bar.close_price - self.trade_high_array
        self.drawdown_array[self.position_array < 0] = self.trade_low_array- bar.close_price
        self.write_log('Current Price: {}'.format(major_snap['close']))

        # run for position change
        if am.inited:
            position_change = [self.strategies[i].next_snap(
                major_snap = major_snap,
                minor_snap = None,
                position   = self.position_array[i],
                drawdown   = self.drawdown_array[i],
                from_sod   = from_sod,
                to_eod     = to_eod,
                is_next_day = is_next_day
            ) for i in range(len(self.strategies))]

            # send out aggregated orders
            self.auto_order(bar.close_price, sum(position_change))
            # print(self.drawdown_array)

            if sum(position_change) != 0:
                self.write_log('Position change: {}'.format(sum(position_change)))
            # # if (bar.datetime.replace(tzinfo = None) >= datetime.datetime(2019, 1, 14, 9, 0, 0)) and (bar.datetime.replace(tzinfo = None) <= datetime.datetime(2019, 1, 14, 9, 10, 0)):
            #     self.count = self.count + 1
            #     print(self.count)
            #     # print(am.daily_close_array)
            #     print(self.pos)
            #     # print(sum(position_change))
            #     print(bar.datetime.replace(tzinfo = None) + datetime.timedelta(minutes=1))
            #     # print(major_snap)
            #     print(self.strategies[0].benchmark)
            #     # print(from_sod)
            #     # print(to_eod)

            self.position_array += position_change

            self.put_event()