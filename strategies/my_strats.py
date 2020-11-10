



# import datetime
# import numpy as np
# from datetime import time
# from data.process_data  import FuturesData
# from vnpy_extension.array_manager import ArrayManager
# from vnpy_extension.config import CACHE_PATH
# from vnpy.app.cta_strategy import (
#     CtaTemplate,
#     StopOrder,
#     TickData,
#     BarData,
#     TradeData,
#     OrderData,
#     BarGenerator,
#     # ArrayManager,
# )



# cython setup
import pyximport
pyximport.install(reload_support= True)

import vnpy_extension.array_manager
# importlib.reload(vnpy_extension.array_manager)

import vnpy_extension.template
# importlib.reload(vnpy_extension.template)

import strategies.cstrategy



ABCCC = type('ABCCC', (vnpy_extension.template.CustomTemplate, ), {
    'strategies': [strategies.cstrategy.TEST(
        scale = 3,
        fix_loss = 10,
    )]
})


