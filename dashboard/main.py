import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.graph_objs as go

import pandas as pd

from dash.dependencies import Input, Output, State

from core.reporting_utils import create_reporting_engine

sql_engine = create_reporting_engine()

embeddings = pd.read_sql_table(
    table_name="embeddings",
    con=sql_engine
)

external_stylesheets = [dbc.themes.BOOTSTRAP]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


def create_main_layout():
    categories_bar = dcc.Graph(
        id='categories_counts',
        style={'height': '100%'}
    )
    years_bar = dcc.Graph(
        id='year_counts',
        style={'height': '100%'}
    )
    categories_embeddings_scatter = dcc.Graph(
        id='embeddings',
        style={'height': '100%', 'width': '100%'}
    )
    return html.Div(children=[
        dcc.Interval(interval=60 * 1000, id="interval"),
        dbc.Row([
            dbc.Col([
                dbc.Row([categories_bar], style={'height': '50vh'}),
                dbc.Row([years_bar], style={'height': '50vh'})
            ], width=4),
            dbc.Col([categories_embeddings_scatter], width=8)
        ], style={'height': '90vh'})
    ])


app.layout = create_main_layout()

app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})


@app.callback(
    Output("categories_counts", "figure"),
    [Input("interval", "n_intervals")],
)
def update_categories_counts(value):
    app.logger.info("Pulling categories counts.")
    embeddings = pd.read_sql_table(
        table_name="embeddings",
        con=sql_engine
    )
    categories = pd.read_sql_table(
        table_name="categories_counts",
        con=sql_engine
    )
    categories_code = embeddings[['category', 'category_code']].drop_duplicates()
    categories = categories.join(categories_code.set_index('category'), on='category')
    return {
        'data': [go.Bar(
            x=categories.category,
            y=categories.cnt,
            marker=dict(color=categories.category_code)
        )],
        'layout': {
            'title': "Number of papers by category"
        }
    }


@app.callback(
    Output("year_counts", "figure"),
    [Input("interval", "n_intervals")],
)
def update_year_counts(value):
    app.logger.info("Pulling year counts.")
    years = pd.read_sql_table(
        table_name="year_counts",
        con=sql_engine
    )
    return {
        'data': [go.Bar(
            x=years.posted_year,
            y=years.cnt
        )],
        'layout': {
            'title': "Number of papers by year"
        }
    }


@app.callback(
    Output("embeddings", "figure"),
    [Input("interval", "n_intervals")],
)
def update_embeddings(value):
    app.logger.info("Pulling embeddings.")
    embeddings = pd.read_sql_table(
        table_name="embeddings",
        con=sql_engine
    )
    return {
        'data': [go.Scatter3d(
            x=embeddings['embedding_reduced_sample'].map(lambda x: x[0]).to_numpy(),
            y=embeddings['embedding_reduced_sample'].map(lambda x: x[1]).to_numpy(),
            z=embeddings['embedding_reduced_sample'].map(lambda x: x[2]).to_numpy(),
            mode='markers',
            hovertext=embeddings['category'].to_numpy(),
            marker=dict(color=embeddings['category_code'].to_numpy())
        )],
        'layout': {
            'title': "Paper's abstract embeddings by category"
        }
    }


if __name__ == '__main__':
    app.run_server(host='0.0.0.0')
