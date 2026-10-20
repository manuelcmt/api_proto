import threading
from collections import Counter
from datetime import date, datetime, timedelta, timezone

import plotly.graph_objects as go
from dash import Dash, Input, Output, dash_table, dcc, html, no_update
from google.cloud import bigquery

import bq

REFRESH_MS = 10000

SURFACE, INK, INK_2, GRID, SERIES = "#fcfcfb", "#0b0b0b", "#52514e", "#e8e7e3", "#2a78d6"

COLUMNS = "registered_on, event_date, terminal, production_line, presentation, unique_n, packer_id, packer_name, packer_group, loaded_at"

_rows: list[dict] = []
_last_loaded = datetime(1970, 1, 1, tzinfo=timezone.utc)
_lock = threading.Lock()


def sync() -> list[dict]:
    """Pulls only rows loaded since the last call. The emulator needs ~2 s per aggregate query, too slow for every refresh."""
    global _last_loaded
    with _lock:
        new = bq.query(f"SELECT {COLUMNS} FROM `{bq.EVENTS}` WHERE loaded_at > @t",
                       [bigquery.ScalarQueryParameter("t", "TIMESTAMP", _last_loaded)])
        if new:
            _rows.extend(new)
            _last_loaded = max(r["loaded_at"] for r in new)
        return _rows


def counts(rows, key) -> list[tuple]:
    return sorted(Counter(key(r) for r in rows).items())


def stats(start: str, end: str, lines: list, pres: list) -> dict:
    start_d, end_d = date.fromisoformat(start), date.fromisoformat(end)
    rows = [r for r in sync()
            if start_d <= r["event_date"] <= end_d
            and (not lines or r["production_line"] in lines)
            and (not pres or r["presentation"] in pres)]
    packers = Counter(r["packer_name"] or f"Unknown #{r['packer_id']}" for r in rows)
    recent = sorted(rows, key=lambda r: r["registered_on"], reverse=True)[:50]
    return {
        "totals": {"events": len(rows), "packers": len({r["packer_id"] for r in rows}),
                   "last_event": max((r["registered_on"] for r in rows), default=None)},
        "daily": [{"event_date": d, "events": n} for d, n in counts(rows, lambda r: r["event_date"])],
        "hourly": [{"hour": h, "events": n} for h, n in
                   counts([r for r in rows if r["event_date"] == end_d], lambda r: r["registered_on"].replace(minute=0, second=0, microsecond=0))],
        "by_line": [{"production_line": k, "events": n} for k, n in counts(rows, lambda r: r["production_line"])],
        "by_presentation": [{"presentation": k, "events": n} for k, n in
                            sorted(Counter(r["presentation"] for r in rows).items(), key=lambda kv: -kv[1])],
        "top_packers": [{"packer": k, "events": n} for k, n in packers.most_common(10)],
        "recent": [{**{k: r[k] for k in ("terminal", "production_line", "presentation", "unique_n", "packer_id", "packer_name", "packer_group")},
                    "registered_on": r["registered_on"].strftime("%Y-%m-%d %H:%M:%S")} for r in recent],
    }


def options() -> dict:
    rows = sync()
    dates = [r["event_date"] for r in rows]
    last = max(dates, default=date.today())
    return {"first": min(dates, default=last), "last": last,
            "lines": sorted({r["production_line"] for r in rows if r["production_line"]}),
            "pres": sorted({r["presentation"] for r in rows if r["presentation"]})}


def bar(title: str, x, y, horizontal: bool = False) -> go.Figure:
    fig = go.Figure(go.Bar(x=y if horizontal else x, y=x if horizontal else y,
                           orientation="h" if horizontal else "v", marker_color=SERIES,
                           marker_line_width=0, hovertemplate="%{y}: %{x}<extra></extra>" if horizontal else "%{x}: %{y}<extra></extra>"))
    fig.update_layout(title=dict(text=title, font=dict(size=15, color=INK)), paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
                      font=dict(color=INK_2, family="system-ui, sans-serif"), margin=dict(l=12, r=12, t=48, b=12),
                      bargap=0.25, height=320)
    fig.update_xaxes(gridcolor=GRID, zeroline=False)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, autorange="reversed" if horizontal else True)
    return fig


def tile(label: str, id_: str):
    return html.Div([html.Div(label, className="label"), html.Div("–", id=id_, className="value")], className="tile")


def control(label: str, id_: str, component):
    return html.Div([html.Label(label, htmlFor=id_, className="label"), component], className="control")


def graph(id_: str):
    return html.Div(className="card", children=dcc.Graph(id=id_, config={"displayModeBar": False}))


app = Dash(__name__, title="Production metrics")
app.index_string = app.index_string.replace("</head>", f"""<style>
body {{ margin:0; background:{SURFACE}; color:{INK}; font-family:system-ui,sans-serif; }}
.wrap {{ max-width:1200px; margin:0 auto; padding:16px; }}
.row {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(280px,1fr)); gap:16px; margin-bottom:16px; }}
.filters {{ display:flex; flex-wrap:wrap; gap:16px; align-items:end; margin-bottom:16px; }}
.control {{ display:flex; flex-direction:column; gap:4px; min-width:220px; }}
.tile {{ border:1px solid {GRID}; border-radius:8px; padding:12px 16px; }}
.label {{ color:{INK_2}; font-size:13px; }} .value {{ font-size:28px; font-weight:600; }}
.card {{ border:1px solid {GRID}; border-radius:8px; overflow:hidden; }}
#status {{ color:#b42318; min-height:20px; }}
</style></head>""")


def serve_layout():
    try:
        o = options()
    except Exception:  # BigQuery not ready: render with empty filters, the refresh callback reports the error
        o = {"first": date.today(), "last": date.today(), "lines": [], "pres": []}
    return html.Div(className="wrap", children=[
        html.H1("Production metrics", style={"fontSize": "22px"}),
        html.Div(className="filters", children=[
            control("Date range", "f-dates", dcc.DatePickerRange(
                id="f-dates", min_date_allowed=o["first"], max_date_allowed=o["last"] + timedelta(days=1),
                start_date=max(o["first"], o["last"] - timedelta(days=6)), end_date=o["last"], display_format="YYYY-MM-DD")),
            control("Production line", "f-lines", dcc.Dropdown(id="f-lines", options=o["lines"], multi=True, placeholder="All lines")),
            control("Presentation", "f-pres", dcc.Dropdown(id="f-pres", options=o["pres"], multi=True, placeholder="All presentations")),
        ]),
        html.Div(id="status", role="status"),
        html.Div(className="row", children=[tile("Boxes packed", "k-events"), tile("Active packers", "k-packers"), tile("Last event", "k-last")]),
        html.Div(className="row", children=[graph("g-daily"), graph("g-hourly")]),
        html.Div(className="row", children=[graph("g-line"), graph("g-pres"), graph("g-packers")]),
        html.H2("Latest events", style={"fontSize": "16px"}),
        dash_table.DataTable(id="t-recent", page_size=15, style_table={"overflowX": "auto"},
                             style_header={"fontWeight": "600", "backgroundColor": SURFACE},
                             style_cell={"fontFamily": "system-ui,sans-serif", "padding": "6px", "backgroundColor": SURFACE, "color": INK}),
        dcc.Interval(id="tick", interval=REFRESH_MS),
    ])


app.layout = serve_layout


@app.callback(
    Output("status", "children"), Output("k-events", "children"), Output("k-packers", "children"),
    Output("k-last", "children"), Output("g-daily", "figure"), Output("g-hourly", "figure"),
    Output("g-line", "figure"), Output("g-pres", "figure"), Output("g-packers", "figure"), Output("t-recent", "data"),
    Input("tick", "n_intervals"), Input("f-dates", "start_date"), Input("f-dates", "end_date"),
    Input("f-lines", "value"), Input("f-pres", "value"),
)
def refresh(_, start, end, lines, pres):
    if not (start and end):
        return ("Pick a start and end date.", *[no_update] * 9)
    try:
        s = stats(start[:10], end[:10], lines, pres)
    except Exception as e:
        return (f"BigQuery unavailable: {e}", *[no_update] * 9)
    t = s["totals"]
    return (
        "",
        f"{t['events']:,}",
        f"{t['packers']:,}",
        t["last_event"].strftime("%Y-%m-%d %H:%M:%S") if t["last_event"] else "–",
        bar("Boxes per day", [r["event_date"] for r in s["daily"]], [r["events"] for r in s["daily"]]),
        bar(f"Boxes per hour on {end[:10]}", [r["hour"] for r in s["hourly"]], [r["events"] for r in s["hourly"]]),
        bar("Boxes by production line", [r["production_line"] for r in s["by_line"]], [r["events"] for r in s["by_line"]]),
        bar("Boxes by presentation", [r["presentation"] for r in s["by_presentation"]], [r["events"] for r in s["by_presentation"]]),
        bar("Top 10 packers", [r["packer"] for r in s["top_packers"]], [r["events"] for r in s["top_packers"]], horizontal=True),
        s["recent"],
    )


if __name__ == "__main__":
    try:
        sync()  # warm the cache so the first page load is fast
    except Exception as e:
        print(f"initial sync failed, will retry on refresh: {e}", flush=True)
    # shortcut: Dash dev server, fine for a single-user demo; use gunicorn for real traffic
    app.run(host="0.0.0.0", port=8050, debug=False)
