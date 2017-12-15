import dash, flask, os
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd

server = flask.Flask(__name__)
server.secret_key = os.environ.get('secret_key', 'secret')
app = dash.Dash(name = __name__, server = server)
app.config.supress_callback_exceptions = True


csv = 'https://www.snap.uaf.edu/webshared/jschroder/WRF_extract_GFDL_1970-2100_FAI.csv'
out_fn = '/workspace/Shared/Users/jschroder/TMP/WRF_extract_GFDL_1970-2100_FAI.csv'
df = pd.read_csv(csv,index_col = 0)
df.index = pd.to_datetime(df.index)
temp = ('C1 : 0 to -25','C2 : -25.1 to -50','C3 : colder than -50')
values = (0 , -25 , -40 )

app.css.append_css({'external_url': 'https://cdn.rawgit.com/plotly/dash-app-stylesheets/2d266c578d2a6e8850ebce48fdb52759b2aef506/stylesheet-oil-and-gas.css'})
app.layout = html.Div([
   html.Div(
        [
            html.H1(
                'WRF Temperature Exploration for DOD project - Fairbanks location',
                className='eight columns',
            ),
            html.Img(
                src="https://www.snap.uaf.edu/sites/all/themes/snap_bootstrap/logo.png",
                className='one columns',
                style={
                    'height': '80',
                    'width': '225',
                    'float': 'right',
                    'position': 'relative',
                },
            ),
        ],
        className='row'
    ),
    html.Div([
        html.Div([
            dcc.Dropdown(
                id='nb_days',
                options=[{'label': 'Consecutive days : {}'.format(i), 'value': i} for i in range(10)],
                value=2
            ),
        ],className='six columns'),
        html.Div([
            dcc.Dropdown(
                id='temperature',
                options=[{'label': 'Temperature below : {}'.format(i), 'value': i} for i in values],
                value=0
            )
        ],className='six columns')

    ]),
    html.Div([
        dcc.Graph(id='indicator-graphic'),
    ],className='eleven columns')
])
@app.callback(
    dash.dependencies.Output('indicator-graphic', 'figure'),
    [dash.dependencies.Input('nb_days', 'value'),
     dash.dependencies.Input('temperature', 'value')])
def update_graph(nb_days,temperature):

    x = df[df.rolling(int(nb_days))['max'].max() <= temperature]
    dff = x.groupby(x.index.year)['max'].count().to_frame('occurences')


    return {
        'data': [go.Bar(
            x=dff.index,
            y=dff['occurences']
        )],
        'layout': go.Layout(
            xaxis={
                'title': 'Years',
                },
            yaxis={
                'title': 'Number of occurences',
                },
            margin={'l': 40, 'b': 40, 't': 10, 'r': 0},
            hovermode='closest'
        )
    }


if __name__ == '__main__':
    app.server.run()
