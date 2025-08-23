import argparse
from pathlib import Path
import dash
from dash.dependencies import Input, Output
from dash import dcc
from dash import html
import dash_cytoscape as cyto
import re


def parse_graph_string(oper8_session_graph_str: str) -> list[dict[str, dict[str, str]]]:
    """
    Convert oper8 session dependency string into dash cytoscape elements.
    """
    graph_str_quoted = re.sub(r"(\w+)", r'"\1"', oper8_session_graph_str)
    graph_dict = eval(graph_str_quoted)
    elements = []
    for node in graph_dict:
        elements.append({"data": {"id": node, "label": node}})
    for source, targets in graph_dict.items():
        for target in targets:
            elements.append({"data": {"source": source, "target": target}})
    return elements


def init_cyto_app(elements: list[dict[str, dict[str, str]]]) -> dash.Dash:
    """
    Initialize dash cytoscape application with the specified elements.
    """
    cyto.load_extra_layouts()
    app = dash.Dash(__name__)

    ### Style sheet
    PRIMARY_COLOR = "#141316"
    SECONDARY_COLOR = "#E3E5E6"
    HIGHLIGHT_COLOR = "#1F63B6"
    NODE_SIZE = 12

    stylesheet = [
        {
            "selector": "node",
            "style": {
                "opacity": 0.9,
                "width": NODE_SIZE,
                "height": NODE_SIZE,
                "shape": "diamond",
                "label": "data(label)",
                "background-color": PRIMARY_COLOR,  # node color
                "color": PRIMARY_COLOR,  # node label color
                "font-size": NODE_SIZE * 0.5,
                "text-events": "yes",  # select node by clicking its label text.
            },
        },
        {
            "selector": "node:selected",
            "style": {
                "background-color": HIGHLIGHT_COLOR,  # node color
                "color": HIGHLIGHT_COLOR,  # node label color
            },
        },
        {
            "selector": "edge",
            "style": {
                "target-arrow-color": SECONDARY_COLOR,
                "target-arrow-shape": "triangle",
                "line-color": SECONDARY_COLOR,
                "arrow-scale": 0.5,
                "width": 0.5,
                "curve-style": "bezier",
                "line-opacity": 0.6,
            },
        },
        {
            "selector": "edge:selected",
            "style": {
                "line-color": HIGHLIGHT_COLOR,
                "target-arrow-color": HIGHLIGHT_COLOR,
                "source-arrow-color": HIGHLIGHT_COLOR,
                "line-opacity": 1,
            },
        },
    ]

    ### App layout
    app.layout = html.Div(
        [
            dcc.Dropdown(
                id="dropdown-layout",
                options=[
                    {"label": "random", "value": "random"},
                    {"label": "grid", "value": "grid"},
                    {"label": "circle", "value": "circle"},
                    {"label": "concentric", "value": "concentric"},
                    {"label": "breadthfirst", "value": "breadthfirst"},
                    {"label": "cose", "value": "cose"},
                    # External layout https://dash.plotly.com/cytoscape/layout#loading-external-layout
                    {"label": "cose-bilkent", "value": "cose-bilkent"},
                    {"label": "cola", "value": "cola"},
                    {"label": "euler", "value": "euler"},
                    {"label": "spread", "value": "spread"},
                    {"label": "dagre", "value": "dagre"},
                    {"label": "klay", "value": "klay"},
                ],
                value="klay",
            ),
            html.Div(
                children=[
                    cyto.Cytoscape(
                        id="cytoscape",
                        elements=elements,
                        style={"height": "95vh", "width": "100%"},
                        stylesheet=stylesheet,
                    )
                ]
            ),
        ]
    )

    ### Callbacks
    @app.callback(Output("cytoscape", "layout"), [Input("dropdown-layout", "value")])
    def update_cytoscape_layout(layout):
        return {"name": layout}

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--oper8-graph-path",
        type=Path,
        required=True,
        help="Path to a file which contains oper8 session dependency graph as a string",
    )
    args = parser.parse_args()
    # TODO validate the input. If it cannot be convert into cytoscape elements, raise error.
    graph_str = args.oper8_graph_path.read_text()
    app = init_cyto_app(parse_graph_string(graph_str))
    app.run(debug=False)
