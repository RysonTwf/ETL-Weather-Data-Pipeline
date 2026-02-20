import os
import pandas as pd
from sqlalchemy import create_engine, text

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# Data layer
# ---------------------------------------------------------------------------

DB_CONN = os.environ.get(
    "WEATHER_DB_CONN",
    "postgresql+psycopg2://admin:password@localhost:5432/weather",
)


def get_data():
    """Return (raw_df, summary_df, error). error is None on success."""
    try:
        engine = create_engine(DB_CONN)
        with engine.connect() as conn:
            raw_df = pd.read_sql(
                text("SELECT * FROM raw_weather ORDER BY city, date"),
                conn,
            )
        with engine.connect() as conn:
            summary_df = pd.read_sql(
                text("SELECT * FROM daily_summary ORDER BY city, date"),
                conn,
            )
        engine.dispose()
        return raw_df, summary_df, None
    except Exception as exc:
        return pd.DataFrame(), pd.DataFrame(), str(exc)


# ---------------------------------------------------------------------------
# Pipeline diagram
# ---------------------------------------------------------------------------

def make_pipeline_figure():
    nodes = [
        # (label, x_center, y_center, width, height)
        ("Open-Meteo\nAPI", 1.0, 2.5, 1.2, 0.7),
        ("extract.py", 2.5, 2.5, 1.2, 0.7),
        ("transform.py", 4.0, 2.5, 1.2, 0.7),
        ("load.py", 5.5, 2.5, 1.2, 0.7),
        ("raw_weather\n(PostgreSQL)\ncity, date\ntemperature_max, temperature_min\nprecipitation, windspeed_max", 7.8, 3.6, 2.0, 1.4),
        ("dbt run", 7.8, 2.5, 1.2, 0.7),
        ("daily_summary\n(PostgreSQL)\ncity, date\navg_temperature\ncreated_at", 7.8, 1.4, 2.0, 1.2),
    ]

    shapes = []
    annotations = []

    for label, cx, cy, w, h in nodes:
        shapes.append(dict(
            type="rect",
            x0=cx - w / 2, x1=cx + w / 2,
            y0=cy - h / 2, y1=cy + h / 2,
            fillcolor="#1e3a5f",
            line=dict(color="#4a9eda", width=2),
        ))
        annotations.append(dict(
            x=cx, y=cy,
            text=label.replace("\n", "<br>"),
            showarrow=False,
            font=dict(color="white", size=10),
            align="center",
        ))

    # Arrows: (x_start, y_start, x_end, y_end)
    arrows = [
        (1.6, 2.5, 1.9, 2.5),   # API → extract
        (3.1, 2.5, 3.4, 2.5),   # extract → transform
        (4.6, 2.5, 4.9, 2.5),   # transform → load
        (6.1, 2.5, 6.8, 3.6),   # load → raw_weather (up)
        (8.8, 3.6, 8.8, 2.85),  # raw_weather → dbt (down, right side)
        (8.8, 2.15, 8.8, 1.95), # dbt → daily_summary (down, right side)
    ]

    for x0, y0, x1, y1 in arrows:
        annotations.append(dict(
            x=x1, y=y1,
            ax=x0, ay=y0,
            xref="x", yref="y",
            axref="x", ayref="y",
            showarrow=True,
            arrowhead=2,
            arrowsize=1.2,
            arrowwidth=2,
            arrowcolor="#4a9eda",
            text="",
        ))

    fig = go.Figure()
    fig.update_layout(
        shapes=shapes,
        annotations=annotations,
        xaxis=dict(range=[0, 10], showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(range=[0, 5], showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor="#0d1b2a",
        paper_bgcolor="#0d1b2a",
        margin=dict(l=20, r=20, t=20, b=20),
        height=420,
    )
    return fig


# ---------------------------------------------------------------------------
# Tab builders
# ---------------------------------------------------------------------------

_PLACEHOLDER_STYLE = {
    "color": "#888",
    "textAlign": "center",
    "padding": "60px",
    "fontSize": "16px",
}

_TABLE_STYLE = {
    "backgroundColor": "#1a2a3a",
    "color": "white",
    "border": "1px solid #2a4a6a",
}

_TABLE_HEADER_STYLE = {
    "backgroundColor": "#0d1b2a",
    "color": "#4a9eda",
    "fontWeight": "bold",
}


def _make_datatable(df, table_id):
    return dash_table.DataTable(
        id=table_id,
        columns=[{"name": c, "id": c} for c in df.columns],
        data=df.to_dict("records"),
        page_size=15,
        style_table={"overflowX": "auto"},
        style_cell=_TABLE_STYLE,
        style_header=_TABLE_HEADER_STYLE,
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#162535"}
        ],
    )


def build_tab_raw(raw_df):
    if raw_df.empty:
        return html.Div(
            "No data yet — run the Airflow DAG to populate raw_weather.",
            style=_PLACEHOLDER_STYLE,
        )

    cities = raw_df["city"].unique()
    temp_fig = go.Figure()
    precip_fig = go.Figure()
    wind_fig = go.Figure()

    for city in cities:
        city_df = raw_df[raw_df["city"] == city].sort_values("date")
        temp_fig.add_trace(go.Scatter(
            x=city_df["date"], y=city_df["temperature_max"],
            name=f"{city} max", mode="lines+markers",
        ))
        temp_fig.add_trace(go.Scatter(
            x=city_df["date"], y=city_df["temperature_min"],
            name=f"{city} min", mode="lines", line=dict(dash="dot"),
        ))
        precip_fig.add_trace(go.Bar(
            x=city_df["date"], y=city_df["precipitation"],
            name=city,
        ))
        wind_fig.add_trace(go.Bar(
            x=city_df["date"], y=city_df["windspeed_max"],
            name=city,
        ))

    for fig, title in [
        (temp_fig, "Temperature Max/Min (°C)"),
        (precip_fig, "Precipitation Sum (mm)"),
        (wind_fig, "Wind Speed Max (km/h)"),
    ]:
        fig.update_layout(
            title=title,
            plot_bgcolor="#0d1b2a",
            paper_bgcolor="#0d1b2a",
            font=dict(color="white"),
            legend=dict(bgcolor="#1a2a3a"),
        )

    return html.Div([
        dcc.Graph(figure=temp_fig),
        dcc.Graph(figure=precip_fig),
        dcc.Graph(figure=wind_fig),
        html.H3("Raw Data", style={"color": "#4a9eda", "marginTop": "20px"}),
        _make_datatable(raw_df, "raw-table"),
    ])


def build_tab_summary(summary_df):
    if summary_df.empty:
        return html.Div(
            "No data yet — run dbt after the DAG has loaded raw_weather.",
            style=_PLACEHOLDER_STYLE,
        )

    cities = summary_df["city"].unique()
    avg_fig = go.Figure()
    precip_fig = go.Figure()
    wind_fig = go.Figure()

    for city in cities:
        city_df = summary_df[summary_df["city"] == city].sort_values("date")
        avg_fig.add_trace(go.Scatter(
            x=city_df["date"], y=city_df["avg_temperature"],
            name=city, mode="lines+markers",
        ))
        if "total_precipitation" in city_df.columns:
            precip_fig.add_trace(go.Bar(
                x=city_df["date"], y=city_df["total_precipitation"],
                name=city,
            ))
        if "max_windspeed" in city_df.columns:
            wind_fig.add_trace(go.Bar(
                x=city_df["date"], y=city_df["max_windspeed"],
                name=city,
            ))

    charts = [dcc.Graph(figure=avg_fig)]
    avg_fig.update_layout(
        title="Average Temperature (°C)",
        plot_bgcolor="#0d1b2a",
        paper_bgcolor="#0d1b2a",
        font=dict(color="white"),
        legend=dict(bgcolor="#1a2a3a"),
    )

    if precip_fig.data:
        precip_fig.update_layout(
            title="Precipitation Sum (mm)",
            plot_bgcolor="#0d1b2a",
            paper_bgcolor="#0d1b2a",
            font=dict(color="white"),
            legend=dict(bgcolor="#1a2a3a"),
        )
        charts.append(dcc.Graph(figure=precip_fig))

    if wind_fig.data:
        wind_fig.update_layout(
            title="Wind Speed Max (km/h)",
            plot_bgcolor="#0d1b2a",
            paper_bgcolor="#0d1b2a",
            font=dict(color="white"),
            legend=dict(bgcolor="#1a2a3a"),
        )
        charts.append(dcc.Graph(figure=wind_fig))

    charts += [
        html.H3("Summary Data", style={"color": "#4a9eda", "marginTop": "20px"}),
        _make_datatable(summary_df, "summary-table"),
    ]

    return html.Div(charts)


# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------

app = dash.Dash(__name__)
app.title = "Weather Pipeline Dashboard"

_TAB_STYLE = {
    "backgroundColor": "#0d1b2a",
    "color": "#888",
    "border": "1px solid #2a4a6a",
    "padding": "10px 20px",
}
_TAB_SELECTED_STYLE = {
    "backgroundColor": "#1e3a5f",
    "color": "white",
    "border": "1px solid #4a9eda",
    "borderBottom": "none",
    "padding": "10px 20px",
    "fontWeight": "bold",
}

_BANNER_HIDDEN = {"display": "none"}
_BANNER_VISIBLE = {
    "backgroundColor": "#3a0f0f",
    "color": "#ff9999",
    "padding": "10px 30px",
    "borderBottom": "1px solid #aa3333",
    "fontSize": "13px",
}

app.layout = html.Div(
    style={"backgroundColor": "#0a1520", "minHeight": "100vh", "fontFamily": "Arial, sans-serif"},
    children=[
        # Fires immediately on page load, then every 5 minutes
        dcc.Interval(id="refresh-interval", interval=5 * 60 * 1000, n_intervals=0),

        # Header
        html.Div(
            style={
                "backgroundColor": "#0d1b2a",
                "padding": "20px 30px",
                "borderBottom": "2px solid #4a9eda",
            },
            children=[
                html.H1(
                    "Weather ETL Pipeline Dashboard",
                    style={"color": "white", "margin": 0, "fontSize": "24px"},
                ),
                html.P(
                    "Monitoring raw_weather and daily_summary tables",
                    style={"color": "#888", "margin": "4px 0 0 0", "fontSize": "13px"},
                ),
            ],
        ),

        # DB error banner (hidden until a connection failure occurs)
        html.Div(id="db-error-banner", style=_BANNER_HIDDEN),

        # Tabs
        dcc.Tabs(
            id="tabs",
            value="data-model",
            style={"backgroundColor": "#0d1b2a"},
            children=[
                dcc.Tab(label="Data Model", value="data-model",
                        style=_TAB_STYLE, selected_style=_TAB_SELECTED_STYLE),
                dcc.Tab(label="Raw Weather", value="raw-weather",
                        style=_TAB_STYLE, selected_style=_TAB_SELECTED_STYLE),
                dcc.Tab(label="Daily Summary", value="daily-summary",
                        style=_TAB_STYLE, selected_style=_TAB_SELECTED_STYLE),
            ],
        ),

        # Data Model panel (static — pipeline diagram never needs refreshing)
        html.Div(
            id="panel-data-model",
            style={"display": "block", "padding": "20px"},
            children=[
                html.H2("Pipeline Architecture", style={"color": "#4a9eda"}),
                dcc.Graph(figure=make_pipeline_figure()),
                html.Div([
                    html.H3("Flow", style={"color": "#4a9eda"}),
                    html.Ol([
                        html.Li("Open-Meteo API — hourly weather forecast fetched by extract.py",
                                style={"color": "white", "marginBottom": "6px"}),
                        html.Li("extract.py — HTTP GET, returns raw JSON",
                                style={"color": "white", "marginBottom": "6px"}),
                        html.Li("transform.py — normalises JSON → (raw_df, summary_df) tuple",
                                style={"color": "white", "marginBottom": "6px"}),
                        html.Li("load.py — INSERT raw_df into raw_weather (ON CONFLICT DO NOTHING)",
                                style={"color": "white", "marginBottom": "6px"}),
                        html.Li("dbt run — drops & recreates daily_summary from raw_weather",
                                style={"color": "white", "marginBottom": "6px"}),
                        html.Li("quality_check — asserts both tables have rows",
                                style={"color": "white", "marginBottom": "6px"}),
                    ], style={"paddingLeft": "20px"}),
                ], style={"backgroundColor": "#0d1b2a", "padding": "20px",
                           "borderRadius": "6px", "marginTop": "10px"}),
            ],
        ),

        # Raw Weather panel — content populated/refreshed by callback
        html.Div(
            id="panel-raw-weather",
            style={"display": "none", "padding": "20px"},
            children=[
                html.H2("Raw Weather", style={"color": "#4a9eda"}),
                html.Div(id="raw-content"),
            ],
        ),

        # Daily Summary panel — content populated/refreshed by callback
        html.Div(
            id="panel-daily-summary",
            style={"display": "none", "padding": "20px"},
            children=[
                html.H2("Daily Summary (dbt)", style={"color": "#4a9eda"}),
                html.Div(id="summary-content"),
            ],
        ),
    ],
)


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

@app.callback(
    Output("panel-data-model", "style"),
    Output("panel-raw-weather", "style"),
    Output("panel-daily-summary", "style"),
    Input("tabs", "value"),
)
def switch_tab(tab):
    base = {"padding": "20px"}
    show = {**base, "display": "block"}
    hide = {**base, "display": "none"}
    return (
        show if tab == "data-model" else hide,
        show if tab == "raw-weather" else hide,
        show if tab == "daily-summary" else hide,
    )


@app.callback(
    Output("raw-content", "children"),
    Output("summary-content", "children"),
    Output("db-error-banner", "children"),
    Output("db-error-banner", "style"),
    Input("refresh-interval", "n_intervals"),
)
def refresh_data(_n):
    raw_df, summary_df, error = get_data()
    if error:
        banner_text = f"Database unreachable — charts show last loaded data. Error: {error}"
        banner_style = _BANNER_VISIBLE
    else:
        banner_text = ""
        banner_style = _BANNER_HIDDEN
    return build_tab_raw(raw_df), build_tab_summary(summary_df), banner_text, banner_style


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
