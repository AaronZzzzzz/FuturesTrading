
import plotly.offline as po
import plotly.graph_objects as go
import plotly.io as pio
import pandas as pd
import numpy  as np

from plotly.subplots import make_subplots

UP_COLOR = '#00FF00'
DOWN_COLOR = '#FF0000'
GRID_COLOR = '#404040'

pio.renderers.default = "browser"

def get_time_profile(df):

    dt        = np.sort(np.unique(df.index.time))
    dt_mins   = np.array([x.hour * 60 + x.minute for x in dt])
    dt_diff_1 = np.diff(dt_mins, prepend = True)
    dt_diff_2 = np.flip(np.diff(np.flip(dt_mins), prepend = True))
    dt_bound  = np.round(dt_mins[np.where(dt_diff_1 + dt_diff_2 != 0)] / 15) / 4
    dt_bound  = dt_bound[[-1] + list(range(len(dt_bound) - 1))]
    return dt_bound.reshape(-1, 2).tolist()


def kplot(df, position = None, wealth = None, k_indicator = None):

    fig = go.Figure()

    # ohlc data
    fig.add_trace(go.Candlestick(
        x     = df.index,
        open  = df.open,
        high  = df.high,
        low   = df.low,
        close = df.close,
        yaxis = 'y2',
        name  = 'OHLC',
    ))

    # volume data
    colors = []
    for i in range(len(df.close)):
        if i != 0:
            if df.close[i] > df.close[i-1]:
                colors.append('#FF0000')
            else:
                colors.append('#00FF00')
        else:
            colors.append('#00FF00')
    fig.add_trace(go.Bar(
        type   = 'bar',
        x      = df.index,
        y      = df.volume,
        yaxis  = 'y',
        name   = 'Volume',
        marker = dict( color=colors ),
    ))

    # signal data
    if position is not None:
        position[-1] = 0
        signal = position.diff().fillna(0)
        signal = signal[signal != 0]
        signal = signal.append(signal[signal.abs() == 2])
        signal[signal.abs() == 2] = signal[signal.abs() == 2] / 2
        signal = signal.sort_index()
        signal_dt = signal.index.tolist()
        signal_dt = [[signal_dt[2*i], signal_dt[2*i + 1]] for i in range(len(signal_dt) // 2)]
        signal_side = signal.values.reshape(-1, 2).tolist()
        signal_count = len(signal_dt)
        for i in range(signal_count):
            delta = df.loc[signal_dt[i][1]].open - df.loc[signal_dt[i][0]].open
            fig.add_trace(go.Scatter(
                x      = [signal_dt[i][0], signal_dt[i][1]],
                y      = [df.loc[signal_dt[i][0]].open, df.loc[signal_dt[i][1]].open],
                yaxis  = 'y2',
                mode   = 'lines+markers',
                marker = dict(color = UP_COLOR if (signal_side[i][0] * delta) > 0 else DOWN_COLOR),
                line   = dict(color = UP_COLOR if (signal_side[i][0] * delta) > 0 else DOWN_COLOR),
                showlegend = False,
            ))
    
    # wealth data
    if wealth is not None:
        fig.add_trace(go.Scatter(
            x     = wealth.index,
            y     = wealth.values,
            yaxis = 'y3',
            name  = 'Wealth',
            mode  = 'lines',
            showlegend = True,
            connectgaps = True,
            line  = dict(color = '#37ded8'),
        ))

    # indicator data
    if k_indicator is not None:
        for ind in k_indicator:
            fig.add_trace(go.Scatter(
                x     = ind.index,
                y     = ind.values,
                yaxis = 'y2',
                name  = 'K_Indicator',
                mode  = 'lines',
                showlegend = True,
                connectgaps = True,
                line  = dict(color = '#fff200'),
            ))

    # layout
    time_profile = get_time_profile(df)
    fig.update_layout(
        plot_bgcolor = 'rgb(0, 0, 0)',
        xaxis = dict(
            rangeselector = dict(visible = True),
            rangebreaks=[
                dict(bounds = ['sat', 'mon'])
            ] + [
                dict(bounds = x, pattern = 'hour') for x in time_profile
            ],
            gridcolor = GRID_COLOR,
        ),
        yaxis = dict( 
            domain = [0, 0.2], 
            showticklabels = False,
        ),
        yaxis2 = dict( 
            domain = [0.2, 0.8],
            side   = 'left',
            gridcolor = GRID_COLOR,
        ),
        yaxis3 = dict( 
            domain = [0.2, 0.8],
            side   = 'right',
            overlaying = 'y2',
        ),
        legend = dict(
            orientation = 'h', 
            y=0.9, 
            x=0.3, 
            yanchor  ='bottom'
        ),
        margin = dict( 
            t = 40, 
            b = 40, 
            r = 40,
            l = 40,
        ),
    )

    
    fig.write_html('stock.html', auto_open = True)
    
# if __name__ == "__main__":
#     from data.process_data import FuturesData

#     # fd = FuturesData('RB')
#     df = fd.get_min_bar(
#         interval = 1, 
#         shift = 0, 
#         start_date = datetime.date(2019, 3, 21), 
#         end_date = datetime.date(2019, 3, 31),
#     )

#     signal = df.iloc[[20, 40, 80, 160]].copy()
#     signal['signal'] = 0
#     signal['signal'].iloc[[0, 2]] = 1
#     signal['signal'].iloc[[1, 3]] = -1 
#     signal['price'] = signal['close']

#     kplot(df, signal)