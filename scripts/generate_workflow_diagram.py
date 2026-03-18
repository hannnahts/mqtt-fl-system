#!/usr/bin/env python3
import argparse
from pathlib import Path
from textwrap import wrap
from xml.sax.saxutils import escape


WIDTH = 1800
MARGIN_X = 120
BOX_W = 1560
TOP_Y = 150
GAP = 28
SECTION_H = 44
ARROW_GAP = 26

FONT = "DejaVu Sans, Liberation Sans, Arial, sans-serif"
MONO = "DejaVu Sans Mono, Liberation Mono, monospace"


STEPS = [
    {
        "number": "1",
        "title": "Prepare Raw Dataset",
        "lines": [
            "Input file: data/raw/NEWC_2025.csv",
            "scripts/prepare_dataset.py keeps PM10, NO, NO2, O3, and PM25, drops missing rows,",
            "and writes data/processed/clean_air.csv",
        ],
        "accent": "#0b7285",
    },
    {
        "number": "2",
        "title": "Split Data For Each Client",
        "lines": [
            "scripts/split_dataset.py divides clean_air.csv into five shards",
            "Outputs: data/splits/client_1.csv ... client_5.csv",
        ],
        "accent": "#0b7285",
    },
    {
        "number": "3",
        "title": "Start MQTT Topology",
        "lines": [
            "Baseline: one broker on port 11883",
            "Proposed: edge-local 11883, cloud-a 12883, cloud-b 13883",
            "edge-local bridges fl/edge/updates/cloud_a/# and fl/edge/updates/cloud_b/# outward,",
            "and receives fl/cloud/control/# plus fl/cloud/global_model back from cloud-b",
        ],
        "accent": "#1971c2",
    },
    {
        "number": "4",
        "title": "Launch Aggregator And Clients",
        "lines": [
            "Aggregator starts first and subscribes to update topics",
            "Five clients start, each loading its own client_i.csv shard",
        ],
        "accent": "#1971c2",
    },
    {
        "number": "5",
        "title": "Launch Orchestrator",
        "lines": [
            "Orchestrator connects to its broker and waits briefly for the rest of the system",
            "Then it begins Round 1 by publishing a start_round control message",
        ],
        "accent": "#1971c2",
    },
    {
        "number": "6",
        "title": "Publish Control Message",
        "lines": [
            "Topic: fl/control/start in baseline, fl/cloud/control/start in proposed mode",
            "Clients subscribe to the control topic and treat it as the round trigger",
        ],
        "accent": "#2b8a3e",
    },
    {
        "number": "7",
        "title": "Clients Train Local Models",
        "lines": [
            "Each client runs linear regression over PM10, NO, NO2, and O3 to predict PM25",
            "Round 1 starts from zeros; later rounds warm start from the latest global model",
            "If the previous global model has not arrived yet, the control message is held pending",
        ],
        "accent": "#2b8a3e",
    },
    {
        "number": "8",
        "title": "Clients Publish Local Updates",
        "lines": [
            "Payload includes client_id, weights, bias, round, route_target, and initial_model_round",
            "Baseline topic: fl/update/client_i on the single edge broker",
            "Proposed topic: fl/edge/updates/cloud_a|cloud_b/client_i on edge-local, gzip enabled",
            "Route selection is round-robin by client_id across cloud_a and cloud_b",
        ],
        "accent": "#2b8a3e",
    },
    {
        "number": "9",
        "title": "Aggregator Collects Round Updates",
        "lines": [
            "Baseline aggregator listens on one broker; proposed aggregator opens MQTT clients to both clouds",
            "Updates are buffered by round until all 5 client updates arrive",
        ],
        "accent": "#ae3ec9",
    },
    {
        "number": "10",
        "title": "Run Federated Averaging And Publish Global Model",
        "lines": [
            "Aggregator computes the average of all client weight vectors and bias terms",
            "Baseline publish topic: fl/global_model on the edge broker",
            "Proposed publish topic: fl/cloud/global_model on cloud-b, gzip enabled, then bridged to edge-local",
        ],
        "accent": "#ae3ec9",
    },
    {
        "number": "11",
        "title": "Clients And Orchestrator Receive Global Model",
        "lines": [
            "Clients cache the newest model for the next local training round",
            "Orchestrator observes the same round completion event through the global model topic",
        ],
        "accent": "#ae3ec9",
    },
]


def wrap_lines(text, width):
    return wrap(text, width=width, break_long_words=False, replace_whitespace=False) or [text]


def build_step_heights():
    heights = []
    for step in STEPS:
        wrapped = []
        for line in step["lines"]:
            wrapped.extend(wrap_lines(line, 72))
        step["_wrapped"] = wrapped
        body_height = 34 + 30 + len(wrapped) * 26 + 26
        heights.append(max(body_height, 132))
    return heights


def text_block(x, y, lines, size=24, fill="#1f2933", weight="400", family=FONT):
    tspans = []
    for idx, line in enumerate(lines):
        dy = "0" if idx == 0 else "1.35em"
        tspans.append(
            f'<tspan x="{x}" dy="{dy}">{escape(line)}</tspan>'
        )
    return (
        f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill}">{"".join(tspans)}</text>'
    )


def rect(x, y, w, h, fill, stroke="none", stroke_width=1, rx=18):
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="{stroke_width}"/>'
    )


def draw_arrow(elements, x1, y1, x2, y2, label=None):
    elements.append(
        f'<path d="M {x1} {y1} L {x2} {y2}" stroke="#5b6472" stroke-width="4" '
        f'fill="none" marker-end="url(#arrow)"/>'
    )
    if label:
        lx = (x1 + x2) / 2 + 12
        ly = (y1 + y2) / 2 - 8
        elements.append(text_block(lx, ly, [label], size=18, fill="#4c5663", weight="600"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    step_heights = build_step_heights()
    phase_b_start_index = 5

    total_height = TOP_Y + 110 + SECTION_H
    total_height += sum(step_heights[:phase_b_start_index]) + GAP * (phase_b_start_index - 1)
    total_height += 72 + SECTION_H
    total_height += sum(step_heights[phase_b_start_index:]) + GAP * (len(STEPS) - phase_b_start_index - 1)
    total_height += 240

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{total_height}" viewBox="0 0 {WIDTH} {total_height}">',
        "<defs>",
        '<linearGradient id="bg" x1="0%" y1="0%" x2="0%" y2="100%">'
        '<stop offset="0%" stop-color="#f4f7fb"/>'
        '<stop offset="100%" stop-color="#eef2f7"/>'
        "</linearGradient>",
        '<linearGradient id="titlebg" x1="0%" y1="0%" x2="100%" y2="0%">'
        '<stop offset="0%" stop-color="#0b7285"/>'
        '<stop offset="100%" stop-color="#1971c2"/>'
        "</linearGradient>",
        '<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="9" markerHeight="9" orient="auto-start-reverse">'
        '<path d="M 0 0 L 10 5 L 0 10 z" fill="#5b6472"/>'
        "</marker>",
        "</defs>",
        rect(0, 0, WIDTH, total_height, "url(#bg)", rx=0),
        rect(MARGIN_X, 48, BOX_W, 110, "url(#titlebg)", rx=28),
        text_block(
            MARGIN_X + 48,
            96,
            ["MQTT Federated Learning System Workflow"],
            size=40,
            fill="#ffffff",
            weight="700",
        ),
        text_block(
            MARGIN_X + 48,
            132,
            [
                "Based on scripts/, app/, configs/, docker-compose, and broker bridge settings",
                "This chart merges setup flow and per-round runtime flow for both baseline and proposed modes",
            ],
            size=18,
            fill="#d9f0ff",
            weight="500",
        ),
    ]

    y = TOP_Y + 44
    svg.append(rect(MARGIN_X, y, BOX_W, SECTION_H, "#dceef2", stroke="#b9dce2", stroke_width=1.5, rx=14))
    svg.append(text_block(MARGIN_X + 24, y + 29, ["Phase A: Data Preparation And Startup"], size=24, fill="#0b4f5d", weight="700"))
    y += SECTION_H + 24

    centers = {}

    for idx, (step, height) in enumerate(zip(STEPS, step_heights)):
        if idx == phase_b_start_index:
            y += 48
            svg.append(rect(MARGIN_X, y, BOX_W, SECTION_H, "#e6f4ea", stroke="#c6e3cf", stroke_width=1.5, rx=14))
            svg.append(text_block(MARGIN_X + 24, y + 29, ["Phase B: One Federated Learning Round"], size=24, fill="#1f5f2d", weight="700"))
            y += SECTION_H + 24

        svg.append(rect(MARGIN_X, y, BOX_W, height, "#ffffff", stroke="#d6dbe4", stroke_width=2, rx=22))
        svg.append(rect(MARGIN_X, y, 16, height, step["accent"], rx=22))
        svg.append(f'<circle cx="{MARGIN_X + 48}" cy="{y + 48}" r="26" fill="{step["accent"]}"/>')
        svg.append(text_block(MARGIN_X + 39, y + 57, [step["number"]], size=24, fill="#ffffff", weight="700"))
        svg.append(text_block(MARGIN_X + 94, y + 48, [step["title"]], size=28, fill="#14202b", weight="700"))
        svg.append(text_block(MARGIN_X + 94, y + 86, step["_wrapped"], size=21, fill="#334155", weight="400"))
        centers[step["number"]] = (MARGIN_X + BOX_W / 2, y + height / 2, y, y + height)
        y += height
        if idx < len(STEPS) - 1:
            draw_arrow(svg, WIDTH / 2, y + 2, WIDTH / 2, y + ARROW_GAP, None)
            y += GAP

    diamond_cx = WIDTH / 2
    diamond_top = y + 20
    diamond_w = 290
    diamond_h = 140
    diamond_points = [
        (diamond_cx, diamond_top),
        (diamond_cx + diamond_w / 2, diamond_top + diamond_h / 2),
        (diamond_cx, diamond_top + diamond_h),
        (diamond_cx - diamond_w / 2, diamond_top + diamond_h / 2),
    ]
    points_attr = " ".join(f"{x},{yy}" for x, yy in diamond_points)
    svg.append(
        f'<polygon points="{points_attr}" fill="#fff4db" stroke="#f08c00" stroke-width="3"/>'
    )
    svg.append(text_block(diamond_cx - 72, diamond_top + 62, ["Final round?"], size=28, fill="#8a4b00", weight="700"))

    draw_arrow(svg, WIDTH / 2, centers["11"][3] + 2, WIDTH / 2, diamond_top - 8, None)

    end_y = diamond_top + 200
    svg.append(rect(WIDTH / 2 - 220, end_y, 440, 92, "#e9f7ef", stroke="#2b8a3e", stroke_width=2.5, rx=26))
    svg.append(text_block(WIDTH / 2 - 118, end_y + 55, ["End Experiment"], size=30, fill="#1f5f2d", weight="700"))
    draw_arrow(svg, WIDTH / 2, diamond_top + diamond_h, WIDTH / 2, end_y - 10, "yes")

    loop_y = diamond_top + diamond_h / 2
    loop_left = MARGIN_X - 24
    step6_top = centers["6"][2]
    svg.append(
        f'<path d="M {diamond_cx - diamond_w / 2} {loop_y} '
        f'L {loop_left} {loop_y} '
        f'L {loop_left} {step6_top + 36} '
        f'L {MARGIN_X - 8} {step6_top + 36}" '
        f'stroke="#5b6472" stroke-width="4" fill="none" marker-end="url(#arrow)"/>'
    )
    svg.append(text_block(loop_left + 18, loop_y - 12, ["no, start next round"], size=18, fill="#4c5663", weight="600"))

    svg.append(
        text_block(
            MARGIN_X,
            total_height - 54,
            [
                "Key runtime facts: clients use warm start after round 1, proposed mode compresses updates and global models with gzip, and the aggregator publishes only after receiving all client updates for the round.",
            ],
            size=18,
            fill="#5b6472",
            weight="500",
            family=FONT,
        )
    )

    svg.append("</svg>")
    output_path.write_text("\n".join(svg), encoding="utf-8")


if __name__ == "__main__":
    main()
