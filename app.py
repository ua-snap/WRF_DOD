import dash, flask, os
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
import pickle

server = flask.Flask(__name__)
server.secret_key = os.environ.get('secret_key', 'secret')
app = dash.Dash(name = __name__, server = server)
app.config.supress_callback_exceptions = True

#relies on pickle file here, might want to change to JSON
dic = pickle.load( open( './data/WRF_extract_GFDL_1970-2100_multiloc_dod.p', "rb" ) )

#the truth has been sent by email to Nancy and contains only Greely
df_greely_historical = pd.read_csv('./data/truth.csv',index_col=0)
df_greely_historical.index = pd.to_datetime( df_greely_historical.index )


app.css.append_css({'external_url': 'https://cdn.rawgit.com/plotly/dash-app-stylesheets/2d266c578d2a6e8850ebce48fdb52759b2aef506/stylesheet-oil-and-gas.css'})
app.layout = html.Div([
   html.Div(
        [
            html.H1(
                'WRF Temperature Exploration for DOD project',
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
                 options=[{'label': 'Consecutive days : {}'.format(i), 'value': i} for i in range(1,10)], #consecutive days selection 
                 value=2
             ),
         ],className='three columns'),
         html.Div([
             dcc.Dropdown(
                 id='temperature',
                 options=[{'label': 'Temperature below : {} celsius'.format(i), 'value': i} for i in range(0,-40, -5)], # temperature threshold selection
                 value=0
             )
         ],className='three columns'),
          html.Div([
              dcc.Dropdown(
                id='location',
                options=[{'label': 'Location: {}'.format(i), 'value': i} for i in dic.keys() ], #location extracted from pickle file
                value='Greely'
              )
          ],className='three columns'),

     ]),
     html.Div([
         dcc.Graph(id='indicator-graphic'),
     ],className='eleven columns')
 ],className='ten columns offset-by-one')
@app.callback(
    dash.dependencies.Output('indicator-graphic', 'figure'),
    [dash.dependencies.Input('nb_days', 'value'),
     dash.dependencies.Input('temperature', 'value'),
     dash.dependencies.Input('location', 'value')])

def update_graph(nb_days, temperature, location):
    def rolling_count_serie(serie , temperature , nb_days):
        '''This function is a non rolling window method, value 1 for number of days obviously doesn't work
        but it is okay for this purpose. Non rolling window was request by the group.'''
        ct = 0
        ls = []
        for i in serie :
            if i <= temperature and ct < nb_days :
                ct +=1
            elif ct == nb_days :
                ct = 1
            else :
                ct = 0

            ls.append(ct)
        return ls
    #Dealing with the actual WRF outputs
    df = dic[ location ].copy()

    df.index = pd.to_datetime( df.index )
    df['count'] = rolling_count_serie( df['max'], temperature , int( nb_days ))

    dff = df[ df['count'] == int(nb_days) ]
    dff = dff.groupby( dff.index.year ).count()

    #Dealing with historical CSV file
    df_greely_historical['count'] = rolling_count_serie( df_greely_historical[ 'max' ], temperature , int( nb_days ))
    df_hist = df_greely_historical[ df_greely_historical[ 'count' ] == int( nb_days ) ]
    df_hist = df_hist.groupby( df_hist.index.year ).count()
    df_hist = df_hist.loc[1970:]

    #we just have historical data for Greely so we only display if Greely is selected
    if location == 'Greely' :
        return {
           'data': [go.Bar(
               x = dff.index,
               y = dff['count'],
               name = 'WRF modeled'
           ),
                    go.Scatter(
               x = dff.index,
               y = df_hist['count'],
               mode = 'markers',
               name = 'Greely historical'
           )],
           'layout': go.Layout(
               xaxis=dict(
                   title =  'Years',
                   range = [1969,2101], #there was some axes issues so hard coded
                   ),
               yaxis={
                   'title': 'Number of occurences',
                   },
               margin={'l': 40, 'b': 40, 't': 10, 'r': 0},
               hovermode='closest'
           )
        }
    else : #without Greely just display bars showing amount of days
        return {
           'data': [go.Bar(
               x=dff.index,
               y=dff['count'],
               name = 'WRF modeled'
           )],
           'layout': go.Layout(
               xaxis={
                   'title': 'Years',
                   'autorange':True,
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
