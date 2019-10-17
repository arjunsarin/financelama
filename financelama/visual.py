# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go

import core

def generate_table(dataframe):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(len(dataframe))]
    )

def generate_time_aggregated_expenses(dataframe):
    data = [go.Scatter(
                x = dataframe.day,
                y = dataframe.value,
                name = 'Rest of world',
                mode = 'markers',
                # TODO Add aggregation
                # transforms = [dict(
                #     type = 'aggregate',
                #     aggregations = [dict(
                #         target='y',
                #         func='sum',
                #         enabled=True
                #     )]

                # )]
    )]

    layout = go.Layout(
                title='Time-aggregated expenses'
            )
    
    
    return dcc.Graph(figure = go.Figure(data, layout))

def main():
    # Get data from core module
    lama = core.main()

    app = dash.Dash(__name__, external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])

    app.layout = html.Div(children=[
        html.H1(children='Financelama'),

        #generate_table(lama.data)
        generate_time_aggregated_expenses(lama.data)
    ])

    app.run_server(debug=True)

if __name__ == '__main__':
    main()