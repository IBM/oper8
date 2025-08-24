# Standard
from pathlib import Path
import argparse
import re

# Third Party
from dash import dcc, html, Patch
from dash.dependencies import Input, Output
import dash
import dash_cytoscape as cyto

### Style sheet
# TODO usage it in other file.
PRIMARY_COLOR = "#141316"
SECONDARY_COLOR = "#E3E5E6"
HIGHLIGHT_COLOR = "#1F63B6"
SECONDARY_HIGHLIGHT_COLOR = "#74A6E4"
UNSELECTED_COLOR = "#CBCBCB"
NODE_SIZE = 30

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
            "font-family": "Roboto Condensed, Helvetica Neue, Helvetica",
            "font-size": NODE_SIZE * 0.5,
            "text-events": "yes",  # select node by clicking its label text.
        },
    },
    {
        "selector": "edge",
        "style": {
            "target-arrow-color": SECONDARY_COLOR,
            "target-arrow-shape": "triangle",
            "line-color": SECONDARY_COLOR,
            "arrow-scale": 0.8,
            "width": 1.2,
            "curve-style": "bezier",
            "line-opacity": 0.6,
        },
    },
]


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
    node_ids = {
        element["data"]["id"] for element in elements if "id" in element["data"]
    }

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

    @app.callback(Output("cytoscape", "stylesheet"), Input("cytoscape", "tapNodeData"))
    def update_stylesheet(tap_node_data):
        """
        Highlight selected node and its connected edges.
        """
        base_stylesheet = stylesheet
        if tap_node_data is not None:
            selected_node_id = tap_node_data["id"]
            highlight_styles = [
                {
                    "selector": f'edge[target="{selected_node_id}"]',
                    "style": {
                        "line-color": SECONDARY_HIGHLIGHT_COLOR,
                        "target-arrow-color": SECONDARY_HIGHLIGHT_COLOR,
                        "source-arrow-color": SECONDARY_HIGHLIGHT_COLOR,
                        "line-opacity": 0.8,
                    },
                },
                {
                    "selector": f'edge[source="{selected_node_id}"]',
                    "style": {
                        "line-color": SECONDARY_HIGHLIGHT_COLOR,
                        "target-arrow-color": SECONDARY_HIGHLIGHT_COLOR,
                        "source-arrow-color": SECONDARY_HIGHLIGHT_COLOR,
                        "line-opacity": 0.8,
                    },
                },
                {
                    "selector": f'node[id="{selected_node_id}"]',
                    "style": {
                        "background-color": HIGHLIGHT_COLOR,
                        "color": HIGHLIGHT_COLOR,
                    },
                },
            ]

            # Grayouts not connected nodes.
            connected_node_ids = set()
            for element in elements:
                element_data = element["data"]
                # element_data represents an edge.
                if "target" in element_data and "source" in element_data:
                    if element_data["target"] == selected_node_id:
                        connected_node_ids.add(element_data["source"])
                    elif element_data["source"] == selected_node_id:
                        connected_node_ids.add(element_data["target"])
            not_connected_node_ids = [
                node_id
                for node_id in node_ids
                if (node_id not in connected_node_ids and node_id != selected_node_id)
            ]
            not_connected_node_styles = [
                {
                    "selector": f'node[id="{not_connected_node_id}"]',
                    "style": {
                        "background-color": UNSELECTED_COLOR,
                        "color": UNSELECTED_COLOR,
                    },
                }
                for not_connected_node_id in not_connected_node_ids
            ]
            return base_stylesheet + highlight_styles + not_connected_node_styles

        return base_stylesheet

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
