
import os
os.chdir('H:/Google Drive/trading/futures')

# cython setup
import pyximport
pyximport.install(reload_support= True)

import datetime
import importlib
import strategies.cstrategy
importlib.reload(strategies.cstrategy)
import strategies.cstrategy


from vnpy.app.cta_strategy.backtesting import BacktestingEngine, OptimizationSetting
from vnpy.app.cta_strategy.strategies.atr_rsi_strategy import (
    AtrRsiStrategy,
)
# from datetime import datetime
# from vnpy_extension.template import CustomTemplate

import vnpy_extension.array_manager
importlib.reload(vnpy_extension.array_manager)

import vnpy_extension.template
importlib.reload(vnpy_extension.template)



# DualThrust = type('DualThrust', (vnpy_extension.template.CustomTemplate, ), {
#     'strategies': [strategies.cstrategy.DUAL_THRUST(
#         n_days = 2,
#         fix_loss = 25,
#         upper_scale = 0.25,
#         lower_scale = 0.75
#     )]
# })


TEST = type('TEST', (vnpy_extension.template.CustomTemplate, ), {
    'strategies': [strategies.cstrategy.TEST(
        scale = 2,
        fix_loss = 25,
    )]
})

engine = BacktestingEngine()
engine.set_parameters(
    vt_symbol="rb88.SHFE",
    interval="1m",
    start=datetime.datetime(2020, 1, 4),
    # end=datetime.datetime(2019, 2, 13),
    end=datetime.datetime(2020, 9, 15),
    rate=0/10000,
    slippage=0.5,
    size=10,
    pricetick=1.0,
    capital=10000,
)
engine.add_strategy(TEST, {})

engine.load_data()
engine.run_backtesting()
df = engine.calculate_result()
engine.calculate_statistics()
engine.show_chart()