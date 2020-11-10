
import numpy as np
from datetime import time
from data.process_data  import FuturesData
from vnpy_adapter.array_manager import ArrayManager
from vnpy_adapter.config import CACHE_PATH
from vnpy.app.cta_strategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
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
        self.cost_array: np.ndarray = np.zeros(len(self.strategies))
        self.drawdown_array: np.ndarray = np.zeros(len(self.strategies))

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
        self.load_bar(10)

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
        mins = bar.datetime.time().hour  * 60 + bar.datetime.time().minute - self.sod
        return mins

    def to_eod(self, bar):
        """ Calculate minutes to sod """
        current_time = bar.datetime.time().hour * 60 + bar.datetime.time().minute
        if current_time > self.eod:
            mins = 1440 - current_time + self.eod
        else:
            mins = current_time - self.eod
        return mins

    def auto_order(self, price, order_size):
        if order_size != 0:
            abs_size = abs(order_size)
            if self.order_size > 0:
                if self.pos >= 0:
                    self.buy(price * 1.01, abs(order_size), stop = False)
                else:
                    if (self.pos + order_size) <= 0:
                        self.cover(price * 1.01, abs(order_size), stop = False)
                    else:
                        self.cover(price * 1.01, abs(self.pos), stop = False)
                        self.buy(price * 1.01, abs(order_size) - abs(self.pos), stop = False)
            else:
                if self.pos <= 0:
                    self.short(price * 0.99, abs(order_size), stop = False)
                else:
                    if (self.pos + order_size) >= 0:
                        self.sell(price * 0.99, abs(order_size), stop = False)
                    else:
                        self.sell(price * 0.99, abs(self.pos), stop = False)
                        self.short(price * 0.99, abs(order_size) - abs(self.pos), stop = False)


    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        self.cancel_all()

        am = self.am
        am.update(bar)
        if not am.inited:
            return

        major_snap = am.transform()
        from_sod   = self.from_sod(bar)
        to_eod     = self.to_eod(bar)

        # update drawdown
        self.trade_high_array = np.maximum(self.trade_high_array, bar.high) * np.abs(self.position_array)
        self.drawdown_array = self.position_array * (self.trade_high_array - bar.close)

        # run for position change
        position_change = [self.strategies[i].next_snap(
            major_snap = major_snap,
            minor_snap = None,
            position   = self.position_array[i],
            drawdown   = self.drawdown_array[i],
            from_sod   = from_sod,
            to_eod     = to_eod
        ) for i in range(len(self.strategies))]

        # send out aggregated orders
        self.auto_order(bar.close_price, sum(position_change))

        self.position_array += position_change

        self.put_event()