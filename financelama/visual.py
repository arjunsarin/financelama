# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import plotly.express as px

import pandas as pd


# ROADMAP Activity heatmap (github-like) see: https://community.plot.ly/t/colored-calendar-heatmap-in-dash/10907/5

def generate_table(dataframe):
    df = dataframe[['day', 'orderer', 'reason', 'value', 'category', 'report']]

    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in df.columns])] +

        # Body
        [html.Tr([
            html.Td(df.iloc[i][col]) for col in df.columns
        ]) for i in range(len(df))]
    )


def generate_monthly_expenses(dataframe):
    """
    Generates aggregated monthly expenses as bar chart.

    Parameters
    ----------
    dataframe : pandas.DataFrame
        Evaluated dataframe with data to display

    Returns
    -------
    dash_core_components.Graph
        Graph to add to dash layout.
    """
    extract = dataframe[['day', 'value']]

    # Sum up values per week, returns object with DatetimeIndex
    extract = extract.groupby(pd.Grouper(key='day', freq='M')).sum()

    # Create diagram and fill with data
    data = [dict(
        type='bar',
        x=extract.index,
        y=extract.value,
        name='Rest of world',
    )]

    layout = go.Layout(
        title='Monthly expenses'
    )

    return dcc.Graph(figure=go.Figure(data, layout))


def generate_pie_chart_expenses(dataframe):
    df = dataframe.loc[dataframe['value'] < 0].copy()
    df['value'] = df['value'].abs()

    return dcc.Graph(figure=px.pie(df, values='value', names='category', title='Expenses'))


def generate_pie_chart_income(dataframe):
    df = dataframe.loc[dataframe['value'] > 0].copy()
    return dcc.Graph(figure=px.pie(df, values='value', names='category', title='Income'))


def start_dashboard(dataframe: pd.DataFrame):
    """
    Creates dashboard as web page and starts local server. Functions generating
    page content are invoked from here.

    Parameters
    ----------
    dataframe : pandas.DataFrame
        Evaluated dataframe with data to display
    """
    app = dash.Dash(__name__, external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])

    app.layout = html.Div(children=[
        html.H1('Financelama'),

        # generate_table(lama.data)
        generate_monthly_expenses(dataframe),

        html.Div([
            html.Div([
                generate_pie_chart_expenses(dataframe)
            ], className="six columns"),

            html.Div([
                generate_pie_chart_income(dataframe)
            ], className="six columns")
        ], className="row"),
        generate_table(dataframe)
    ])

    app.run_server(debug=False)
