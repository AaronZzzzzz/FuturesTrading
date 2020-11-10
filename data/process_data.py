
import datetime
import pathlib
import itertools
import talib
import math
import time
import pandas as pd
import numpy  as np
from collections import defaultdict

class FuturesData:

    def __init__(self, sym, path = None, start_date = None, end_date = None):

        self.sym  = sym
        self.path = path

        self._load_data(start_date = start_date, end_date = end_date)

    def _load_data(self, start_date = None, end_date = None):
        """ Function to load historical data """
        load_path = pathlib.Path(self.path) if isinstance(self.path, str) else pathlib.Path.cwd() / 'cache'
        with pd.HDFStore(load_path / '{}.h5'.format(self.sym.upper()), 'r') as load_obj:

            # load calendar
            self.calendar = pd.read_hdf(load_obj, 'calendar')
            start_date = start_date.strftime('%Y-%m-%d') if start_date is not None else self.calendar[0]
            end_date   = end_date.strftime('%Y-%m-%d')   if end_date   is not None else self.calendar[-1]
            self.calendar = self.calendar[(self.calendar >= start_date) & (self.calendar <= end_date)]

            # load liquidity
            self.liquidity = pd.read_hdf(load_obj, 'liquidity')
            self.liquidity = self.liquidity.rolling(3).mean().shift(1).bfill().loc[self.calendar.index]
            self.liquidity = self.liquidity.apply(lambda x: x.nlargest(2), axis = 1).fillna(0)
            self.liquidity = self.liquidity.loc[:, self.liquidity.any()]

            # load market data
            self.market_data = dict()
            self.resample    = dict()
            self.daily       = dict()

            self.dt          = None
            for sym in self.liquidity.columns:
                date_to_load = self.calendar.loc[self.liquidity[sym].index[self.liquidity[sym].to_numpy().nonzero()]].values
                data = {date: pd.read_hdf(load_obj, '{}/{}'.format(sym, date)) for date in date_to_load}
                self.market_data.update({sym: data})

                # create resample: interval=1|shift=0
                resample = pd.concat([v for v in data.values()]).sort_index()
                self.dt = resample.index if self.dt is None else self.dt.union(resample.index)
                self.resample.update({sym: {'interval=1|shift=0': resample}})
                self.daily.update({sym:  
                    pd.DataFrame.from_dict({v.index[0]: {
                        'open': v['open'][0],
                        'close': v['close'][-1],
                        'high': v['high'].max(),
                        'low': v['low'].min(),
                        'volume': v['volume'].sum(),
                        'open_interest': v['open_interest'][-1],
                        } 
                        for k, v in data.items()
                    }, orient = 'index')
                })
            
            # align resample data index
            for k, v in self.resample.items():
                self.resample[k]['interval=1|shift=0'] = pd.DataFrame(v['interval=1|shift=0'], index = self.dt)
    
            # initiate cache
            self.cache = {k: pd.DataFrame() for k in self.liquidity.columns}

            # create reference
            self._generate_dt_profile()

    def _generate_dt_profile(self):
        """ Function to generate datetime profile """
        sym_to_use = self.liquidity.idxmax(axis = 1)
        profile = []
        # last_major_sym = None
        # self.next_day = True
        for date, sym in sym_to_use.items():
            # if sym == last_major_sym:
            #     dt_move_major = False
            # else:
            #     dt_move_major = True

            # total_mins   = date.hour * 60 + date.minute
            date         = date.date().strftime('%Y-%m-%d')

            dt_index     = self.market_data[sym][date].index
            dt_day       = dt_index[dt_index > '{} 08:00:00'.format(date)]
            dt_night     = dt_index[dt_index < '{} 08:00:00'.format(date)]
            dt_day_sod   = dt_day[0]
            dt_day_eod   = dt_day[-1]
            dt_night_sod = dt_night[0] if len(dt_night) > 0 else dt_day[0]
            dt_night_eod = dt_night[-1] if len(dt_night) > 0 else dt_day[-1]

            dt_profile = pd.DataFrame(index = dt_index)
            dt_profile['date']           = date
            dt_profile['is_next_day']    = [True] + [False] * (len(dt_index) - 1)
            dt_profile['major']          = sym
            dt_profile['has_night']      = len(dt_night) > 0
            dt_profile['is_broken_day']  = (dt_index[-1] - dt_index[0]).days > 0
            dt_profile['is_night_sod']   = dt_index == dt_night_sod
            dt_profile['is_night_eod']   = dt_index == dt_night_eod
            dt_profile['is_day_sod']     = dt_index == dt_day_sod
            dt_profile['is_day_eod']     = dt_index == dt_day_eod

            dt_profile['from_night_sod'] = ((dt_index - dt_night_sod).total_seconds() / 60 + 1).astype(int)
            dt_profile['from_day_sod']   = ((dt_index - dt_day_sod).total_seconds() / 60 + 1).astype(int)
            
            dt_profile['to_night_eod']   = ((dt_night_eod - dt_index).total_seconds() / 60).astype(int)
            dt_profile['to_day_eod']     = ((dt_day_eod - dt_index).total_seconds() / 60).astype(int)

            profile += [dt_profile]

            # if self.next_day:
            #     self.next_day = False
            # else:
            #     self.next_day = total_mins == 900
            # last_major_sym = sym

        profile = pd.concat(profile, axis = 0)
        profile = pd.DataFrame(profile, index = self.dt)
        self.dt_profile = profile

    def _resample(self, df, interval, shift = 0, trim = False):
        """ Function to resample time series data """
        if interval > 1:
            df = df.reset_index()
            df['grouper'] = 1 + df.index // interval
            df['grouper'] = df['grouper'].shift(shift).fillna(0)
            df = df.groupby('grouper').aggregate({
                'datetime'     : 'last',
                'open'         : 'first', 
                'close'        : 'last', 
                'high'         : 'max', 
                'low'          : 'min', 
                'volume'       : 'sum', 
                'open_interest': 'last',
            }).dropna(axis = 0, how = 'any').set_index('datetime')
            if trim:
                if shift > 0:
                    df = df.iloc[1:]
                df = df.iloc[:-1]
        return df

    def get_market_data(self, start_date = None, end_date = None, adjust = False, adjust_method = 'forward'):
        """ Function to get minute bar """
        start_date = pd.to_datetime(start_date) if start_date is not None else self.calendar.index[0]
        end_date   = pd.to_datetime(end_date)   if end_date   is not None else self.calendar.index[-1]
        date_range = self.calendar[(self.calendar.index >= start_date) & (self.calendar.index <= end_date)]
        liquidity = self.liquidity.loc[date_range.index]
        major_sym = liquidity.idxmax(axis = 1)
        data = []

        if adjust:
            last_sym   = None
            last_close = None
            adj_value  = 0
            for date, sym in major_sym.items():
                this_data  = self.market_data[sym][self.calendar.loc[date]].copy()
                if sym != last_sym:
                    this_open  = this_data['open'][0]
                    gap        = last_close - this_open if last_close is not None else 0
                    adj_value  += gap
                last_close = this_data['close'][-1]
                last_sym   = sym
                this_data[['open', 'close', 'high', 'low']] += adj_value
                this_data['sym'] = sym
                data += [this_data]
        else:
            adj_value = 0
            for date, sym in major_sym.items():
                this_data = self.market_data[sym][self.calendar.loc[date]].copy()
                this_data['sym'] = sym
                data += [this_data]
        output = pd.concat(data, axis = 0).sort_index()
        if adjust_method == 'backward':
            output[['open', 'close', 'high', 'low']] -= adj_value
        return output
            
    def cache_resample(self, interval = 1, shift = 0):
        """ Function to cache resample data into memory """
        interval = interval if isinstance(interval, list) else [interval]
        shift    = shift   if isinstance(shift, list)     else [shift]
        for i, s in list(itertools.product(interval, shift)):
            for sym in self.market_data.keys():
                resample_name = 'interval={interval}|shift={shift}'.format(
                    interval = i,
                    shift    = s
                )
                if resample_name not in self.resample[sym]:
                    t = time.time()
                    data = [self._resample(v, i, s, trim = False) for v in self.market_data[sym].values()]
                    data = pd.concat(data, axis = 0)
                    data = pd.DataFrame(data, index = self.dt)
                    self.resample[sym].update({resample_name: data})
                    # append += [data]
                    print('Resample "{sym}|{resample_name}" takes {time:.2f} s'.format(
                        sym = sym,
                        resample_name = resample_name, 
                        time = time.time() - t)
                    )

    def cache_custom(self, func_dict):
        """ Function to cache custom data """
        # format: func_dict = {name: {f: <func>, interval: 1, shift: 0}}
        for sym in self.market_data.keys():
            append = []
            for k, v in func_dict.items():
                daily    = v.get('daily', False)
                if not daily:
                    interval = v.get('interval', 1)
                    shift    = v.get('shift', 0)
                
                    resample_name = 'interval={interval}|shift={shift}'.format(
                        interval = interval,
                        shift    = shift
                    )
                    cache_name = k
                    if resample_name not in self.resample[sym]:
                        self.cache_resample(interval = interval, shift = shift)
                    if cache_name not in self.cache[sym]:
                        t = time.time()
                        resample_data = self.resample[sym].get(resample_name).dropna()
                        data = v['func'](resample_data)
                        data = pd.DataFrame(data, index = self.dt)
                        append += [data]
                        print('Intraday Cache "{}" for {} takes {:.2f} s'.format(cache_name, sym, time.time() - t))
                else:
                    cache_name = k
                    if cache_name not in self.cache[sym]:
                        t = time.time()
                        data = v['func'](self.daily[sym]).shift(1)
                        data = pd.DataFrame(data.shift(0), index = self.dt).ffill()
                        append += [data]
                        print('Daily Cache "{}" for {} takes {:.2f} s'.format(cache_name, sym, time.time() - t))

            self.cache[sym] = pd.concat([self.cache[sym]] + append, axis = 1).loc[self.dt]
        self.cache_columns = {k: v.columns.tolist() for k, v in self.cache.items()}

    def prepare_snap(self):
        
        t = time.time()
        self.all_snap = []
        self.all_reference = []
        all_syms = self.liquidity.columns.tolist()
        all_dict_cache = {}
        for sym in all_syms:
            tmp_cache = pd.concat((self.cache[sym], self.resample[sym]['interval=1|shift=0']), axis = 1)
            tmp_data  = tmp_cache[~tmp_cache.isnull().all(axis = 1)]
            index     = tmp_data.index
            columns   = tmp_data.columns
            values    = tmp_data.values
            all_dict_cache[sym]  = {index[i]: dict(zip(columns, values[i])) for i in range(len(index))}
        
        index   = self.dt_profile.index
        columns = self.dt_profile.columns
        values  = self.dt_profile.values
        reference_dict = {index[i]: dict(zip(columns, values[i])) for i in range(len(index))}

        for i in self.dt:
            self.all_snap += [{ 
                sym: {k: float(v) for k, v in all_dict_cache[sym].get(i, {}).items() if not math.isnan(v)}
                for sym in all_syms
            }]
            self.all_reference += [reference_dict[i]]
        print(time.time()- t)


    def snap_cache(self, index = None): #, dt_index = None):
        """ Function to snap cache data """
        # if index is None:
        #     index = self.dt.get_loc(dt_index)
        snap = self.all_snap[index]
        reference = self.all_reference[index]

        # snap = dict()
        # for sym, data in self.cache.items():
        #     # snap[sym] = data.iloc[index].to_dict()
        #     snap[sym] = defaultdict(float)
        #     snap[sym].update(dict(zip(self.cache_columns[sym], data.values[index])))
        #     snap[sym].update(self.resample[sym]['interval=1|shift=0'].iloc[index].to_dict())
        #     # snap[sym]['market'] = self.resample[sym]['interval=1|shift=0'].iloc[index].to_dict()
        #     snap[sym] = {k: float(v) for k, v in snap[sym].items() if not math.isnan(v)}
        # reference = self.dt_profile.iloc[index].to_dict()
        
        return snap, reference
