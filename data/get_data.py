

import h5py
import datetime
import pathlib
import rqdatac as rqd
import pandas  as pd
rqd.init()

START_DATE = datetime.date(2010, 1, 1)

def update_market_data(sym, path = None):
    """ Function to update market data """

    # set up directory
    save_path = pathlib.Path(path) if isinstance(path, str) else pathlib.Path.cwd() / 'cache'
    save_obj  = save_path / '{}.h5'.format(sym.upper())
    
    # open hdf5 handle
    if save_obj.is_file():
        calendar = pd.read_hdf(save_obj, 'calendar')
        liquidity = pd.read_hdf(save_obj, 'liquidity')
    else:
        calendar = pd.Series(name = 'calendar')
        liquidity = pd.DataFrame()
    
    # set start date
    update_date = calendar.index.max() + datetime.timedelta(1) if len(calendar) > 0 else START_DATE 
    
    # set fields
    fields = [
        'open',
        'close',
        'high',
        'low',
        'volume',
        'open_interest',
    ]

    # update for each date
    while update_date <= datetime.date.today():
        
        # datetime to string
        dt_str = update_date.strftime('%Y-%m-%d')

        # get all available contracts
        contracts = rqd.futures.get_contracts(sym, dt_str)

        data = rqd.get_price(
            order_book_ids = contracts, 
            start_date     = dt_str,
            end_date       = dt_str,
            frequency      = '1m', 
            fields         = fields, 
            adjust_type    = 'none', 
            skip_suspended = False, 
            market         = 'cn', 
            expect_df      = True,
        )

        # check if data is valid
        if data is not None:
            
            # reformat data
            data = data.reset_index()
            data_by_sym = {sym: data[data.order_book_id == sym].set_index('datetime')[fields] for sym in contracts}
            data_by_sym = {k: v for k, v in data_by_sym.items() if len(v) > 0}
            
            # update calendar
            calendar.set_value(pd.to_datetime(update_date), dt_str)

            # update liquidity
            liquidity_data = {k: v['open_interest'].iloc[-1] for k, v in data_by_sym.items()}
            liquidity_data = pd.Series(liquidity_data, name = pd.to_datetime(update_date))
            liquidity = liquidity.append(liquidity_data).fillna(0)

            # save to h5
            calendar.to_hdf(save_obj, 'calendar', complib='blosc:zstd', complevel=9)
            liquidity.to_hdf(save_obj, 'liquidity', complib='blosc:zstd', complevel=9)
            for k, v in data_by_sym.items():
                v.to_hdf(save_obj, '{}/{}'.format(k, dt_str), complib='blosc:zstd', complevel=9)
            print('{} update successfully'.format(update_date.strftime('%Y-%m-%d')))
        else:
            print('{} is not a valid trading day'.format(update_date.strftime('%Y-%m-%d')))
            
        # next day
        update_date += datetime.timedelta(1)

if __name__ == "__main__":
    update_market_data('RB')    
