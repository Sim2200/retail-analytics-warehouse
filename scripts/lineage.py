"""Draw the dbt lineage graph (sources -> staging -> intermediate -> snapshot -> marts)
from target/manifest.json as a PNG for the README. Tests are left out.

    python scripts/lineage.py [--manifest dbt/target/manifest.json] [--out docs/lineage.png]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import networkx as nx  # noqa: E402

LAYERS = ["source", "staging", "intermediate", "snapshots", "marts", "analyst"]
COLORS = {"source": "#898781", "staging": "#2a78d6", "intermediate": "#1baf7a", "snapshots": "#eda100",
          "marts": "#eb6834", "analyst": "#4a3aa7"}


def layer_of(node: dict) -> str:
    if node["resource_type"] == "source":
        return "source"
    if node["resource_type"] == "snapshot":
        return "snapshots"
    return node.get("schema", "marts")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", type=Path, default=Path("dbt/target/manifest.json"))
    ap.add_argument("--out", type=Path, default=Path("docs/lineage.png"))
    a = ap.parse_args()
    m = json.loads(a.manifest.read_text())
    nodes = {**m["nodes"], **m["sources"]}
    keep = {k: v for k, v in nodes.items() if v["resource_type"] in ("model", "snapshot", "source")}
    g = nx.DiGraph()
    for k, v in keep.items():
        g.add_node(k, label=v["name"], layer=layer_of(v))
    for k, v in keep.items():
        for dep in v.get("depends_on", {}).get("nodes", []):
            if dep in keep:
                g.add_edge(dep, k)

    # Column per layer, nodes spread vertically within it.
    pos = {}
    for i, layer in enumerate(LAYERS):
        members = sorted((n for n, d in g.nodes(data=True) if d["layer"] == layer), key=lambda n: g.nodes[n]["label"])
        for j, n in enumerate(members):
            pos[n] = (i * 2.6, -(j - (len(members) - 1) / 2) * 1.0)

    fig, ax = plt.subplots(figsize=(16, 7.5))
    fig.patch.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")
    # Edges first, as curved arrows that stop short of the label boxes.
    for u, v in g.edges():
        (x1, y1), (x2, y2) = pos[u], pos[v]
        ax.annotate("", xy=(x2 - 0.75, y2), xytext=(x1 + 0.75, y1),
                    arrowprops=dict(arrowstyle="-|>", color="#c3c2b7", lw=1.2, shrinkA=0, shrinkB=0,
                                    connectionstyle="arc3,rad=0.12"))
    # Nodes as rounded label boxes sized to their text, one colour per layer.
    for n, d in g.nodes(data=True):
        x, y = pos[n]
        ax.text(x, y, d["label"], ha="center", va="center", fontsize=8.5, color="white", fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.45", fc=COLORS[d["layer"]], ec="none"))
    for i, layer in enumerate(LAYERS):
        ax.text(i * 2.6, 4.6, layer, ha="center", fontsize=11, color="#52514e", fontweight="bold")
    ax.set_xlim(-1.2, (len(LAYERS) - 1) * 2.6 + 1.2)
    ax.set_ylim(-4.6, 5.1)
    ax.axis("off")
    ax.set_title("dbt lineage: raw sources to analyst views", loc="left", fontsize=13, fontweight="bold", color="#0b0b0b")
    a.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(a.out, dpi=150, bbox_inches="tight")
    print(f"wrote {a.out} ({g.number_of_nodes()} nodes, {g.number_of_edges()} edges)")


if __name__ == "__main__":
    main()
