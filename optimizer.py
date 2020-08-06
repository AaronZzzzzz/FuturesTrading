
import itertools
import gurobipy as gp
import numpy    as np
import pandas   as pd
from gurobipy import GRB
# from backtest import IntradayBacktest

class PortfolioOptimizer:

    def __init__(self, wealth):

        self.strategy_return = wealth.diff().fillna(0)
        self.strategy_wealth = wealth
        
    def solve(self,
        n_strategies,
        return_lambda = 1.0,
        start_date    = None,
        end_date      = None):
        """ Funtion to solve optimal strategies combination """

        start_date = self.strategy_return.index.min().date() if start_date is None else start_date
        start_date = start_date.strftime('%Y-%m-%d')
        end_date   = self.strategy_return.index.max().date() if end_date   is None else end_date
        end_date   = end_date.strftime('%Y-%m-%d')
        
        strategy_return = self.strategy_return.loc[self.strategy_return.index >= start_date]
        strategy_return = self.strategy_return.loc[self.strategy_return.index <= end_date]
        
        # mean = strategy_return.stack().mean()
        # std  = strategy_return.stack().std()
        # strategy_return = strategy_return.clip(mean - 3 * std, mean + 3 * std)

        covariance = strategy_return.cov()
        exp_return = strategy_return.sum()

        # build up gurobi model
        m   = gp.Model('strategies_combination')
        x   = pd.Series(m.addVars(exp_return.index, vtype=GRB.BINARY), index = exp_return.index)
        obj = covariance.dot(x).dot(x) - return_lambda * exp_return.dot(x)
        m.setObjective(obj , GRB.MINIMIZE)
        m.addConstr(x.sum() == int(n_strategies))
        m.optimize()
        opt_portfolio = pd.Series({k: v.x for k, v in x.items()})
        opt_portfolio = opt_portfolio[opt_portfolio > 0]
        return opt_portfolio


# if __name__ == "__main__":
    # from data.process_data import FuturesData
    # from strategy import SMA
    # fd = FuturesData(
    #     sym = 'RB',
    #     start_date = datetime.date(2019, 3, 1),
    #     end_date = datetime.date(2019, 3, 31), 
    # )

    # generator_config = {
    #     'SMA': {
    #         'mod': SMA,
    #         'args': {
    #             'n_periods': [3, 5, 8, 13],
    #             'interval' : [3, 5, 8, 10],
    #             'shift'    : [0, 1, 2, 3],
    #         }
    #     }
    # }
    # sg = StrategyGenerator(fd)
    # strategies = sg.generate(generator_config, start_date, end_date)
    # po = PortfolioOptimizer(iee.get_history())
    # opt_portfolio = po.solve(10)


