import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.graph_objs as go

import pandas as pd

embeddings = pd.read_json("/data/embeddings_reduced.json")
categories = pd.read_json("/data/categories_stats.json")

categories_code = embeddings[['category', 'category_code']].drop_duplicates()
categories = categories.join(categories_code.set_index('category'), on='category')

external_stylesheets = [dbc.themes.BOOTSTRAP]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

categories_bar = dcc.Graph(
    id='categories',
    style={'height': '100%'},
    figure={
        'data': [go.Bar(
            x=categories.category,
            y=categories.cnt,
            marker=dict(color=categories.category_code)
        )],
        'layout': {
            'title': "Number of papers by category"
        }
    }
)

categories_embeddings_scatter = dcc.Graph(
    id='scatter',
    style={'height': '100%', 'width': '100%'},
    figure={
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
)

app.layout = html.Div(children=[
    dbc.Row([
        dbc.Col([categories_bar], width=4),
        dbc.Col([categories_embeddings_scatter], width=8)
    ], style={'height': '90vh'})
])

app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

if __name__ == '__main__':
    app.run_server(host='0.0.0.0')
