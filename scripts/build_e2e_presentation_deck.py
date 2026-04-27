#!/usr/bin/env python3
"""
Build HL7_Streaming_E2E_Walkthrough.pptx (Python stdlib, same OOXML approach as
scripts/build_evaluation_deck.py). Import into Google Slides: File → Import slides.

Usage:
  python3 scripts/build_e2e_presentation_deck.py
"""

from __future__ import annotations

import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "docs" / "presentations" / "vendor" / "python_pptx_test_template.pptx"
OUT_DIR = ROOT / "docs" / "presentations"
OUT_PATH = OUT_DIR / "HL7_Streaming_E2E_Walkthrough.pptx"

TITLE_PLACEHOLDER = "Presentation Title Text"
SUB_PLACEHOLDER = "Subtitle Text"

NS = {
    "p": "http://schemas.openxmlformats.org/presentationml/2006/main",
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}

for prefix, uri in NS.items():
    ET.register_namespace(prefix, uri)


def _set_slide_texts(slide_path: Path, title: str, subtitle: str) -> None:
    tree = ET.parse(slide_path)
    root = tree.getroot()
    for t in root.iter(f"{{{NS['a']}}}t"):
        if t.text == TITLE_PLACEHOLDER:
            t.text = title
        elif t.text == SUB_PLACEHOLDER:
            t.text = subtitle
    tree.write(slide_path, encoding="UTF-8", xml_declaration=True)


def _next_creation_id(slide_path: Path) -> None:
    raw = slide_path.read_text(encoding="utf-8")
    raw2 = re.sub(
        r'(<p14:creationId[^>]*val=")(\d+)(")',
        lambda m: f"{m.group(1)}{int(m.group(2)) ^ 0x9E3779B9 & 0xFFFFFFFF}{m.group(3)}",
        raw,
        count=1,
    )
    slide_path.write_text(raw2, encoding="utf-8")


def _max_r_id(rels_text: str) -> int:
    ids = [int(x) for x in re.findall(r'Id="rId(\d+)"', rels_text)]
    return max(ids) if ids else 0


def _append_slide_rels(rels_text: str, slide_num: int, r_id: int) -> str:
    needle = "</Relationships>"
    insert = (
        f'<Relationship Id="rId{r_id}" '
        f'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide" '
        f'Target="slides/slide{slide_num}.xml"/>'
    )
    return rels_text.replace(needle, insert + needle)


def _append_content_types(ct_text: str, slide_num: int) -> str:
    needle = "</Types>"
    insert = (
        f'<Override PartName="/ppt/slides/slide{slide_num}.xml" '
        f'ContentType="application/vnd.openxmlformats-officedocument.presentationml.slide+xml"/>'
    )
    return ct_text.replace(needle, insert + needle)


def _append_sld_ids(pres_text: str, slide_num: int, sld_id: int, r_id: int) -> str:
    close = "</p:sldIdLst>"
    insert = f'<p:sldId id="{sld_id}" r:id="rId{r_id}"/>'
    if insert in pres_text:
        return pres_text
    return pres_text.replace(close, insert + close)


def _slide_specs() -> list[tuple[str, str]]:
    b = " \u2022 "

    return [
        (
            "HL7 streaming on Databricks",
            "An end-to-end walkthrough: ingest \u2192 medallion \u2192 serve"
            "\nUnity Catalog \u00b7 Delta Live Tables \u00b7 Lakeview \u00b7 App \u00b7 Lakebase",
        ),
        (
            "What you will see",
            b.join(
                [
                    "A simple mental model: seven stops from file to insight",
                    "How raw HL7 becomes trusted tables, forecasts, and dashboards",
                    "Where the ADT Lakeview dashboard and Streamlit app fit in",
                ]
            ),
        ),
        (
            "Step 1: Land HL7 in the Lakehouse",
            b.join(
                [
                    "Raw HL7 v2.x (files or streams) \u2192 Unity Catalog volume (e.g. landing/)",
                    "Single control plane: catalog, rights, and lineage in UC",
                    "Foundation for repeatable parsing — not ad hoc one-off ETL",
                ]
            ),
        ),
        (
            "Step 2: Bronze \u2014 parse and keep lineage",
            b.join(
                [
                    "Delta Live Tables (DLT) splits multi-message files and parses with funke",
                    "One row per message/segment; schema reflects raw reality (e.g. ADT, ORU)",
                    "Example: bronze.ensemble.ens_adt holds parsed ADT for downstream work",
                ]
            ),
        ),
        (
            "Step 3: Silver \u2014 structure the segments",
            b.join(
                [
                    "Typed MSH, PID, PV1, OBX, etc. for analytics-safe joins",
                    "Quality and normalization early — fewer surprises in gold",
                ]
            ),
        ),
        (
            "Step 4: Gold \u2014 business-ready facts",
            b.join(
                [
                    "Dimensional models: patient, encounter, observations, orders, etc.",
                    "ED/ICU and operational metrics live beside clinical facts in UC",
                    "Governed tables = one source of truth for BI and apps",
                ]
            ),
        ),
        (
            "Step 5: Report & forecast layers",
            b.join(
                [
                    "Real-time style reports: census, hourly and daily rollups in DLT",
                    "Feature tables and AutoML; batch inference; MLflow for versions",
                    "Accuracy and monitoring tables close the loop for capacity planning",
                ]
            ),
        ),
        (
            "Step 6: Serve \u2014 two read paths (by design)",
            b.join(
                [
                    "SQL warehouse \u2192 Lakeview dashboards (broad, warehouse-native SQL)",
                    "Lakebase (Postgres) \u2192 Databricks Streamlit app (fast row reads, filters)",
                    "Grants and service principals line up with Unity Catalog for both paths",
                ]
            ),
        ),
        (
            "ADT Lakeview dashboard (importable pack)",
            b.join(
                [
                    "File: dashboards/adt_ens_bronze.lvdash.json \u00b7 table: ens_adt",
                    "Row-level ds_adt_all (365d) with date and dimension filters on every page",
                    "Three pages: Overview \u00b7 Time (hour, A01) \u00b7 Location and billing",
                    "Regenerate after edits: `python3 scripts/generate_adt_ens_lvdash.py`",
                ]
            ),
        ),
        (
            "Streamlit app + optional Genie",
            b.join(
                [
                    "HL7 app: multi-page experience on Lakebase-backed queries",
                    "Genie (if enabled): natural language to SQL in a governed space",
                    "Same gold tables, different entry points: drag-and-click vs. ask a question",
                ]
            ),
        ),
        (
            "Operations: bundles, jobs, and trust",
            b.join(
                [
                    "Databricks Asset Bundle (IaC) for pipelines and jobs in dev/stage/prod",
                    "Grants: UC USE/SELECT, warehouse access, optional 12_genie_uc_grants job",
                    "Conversations stay inside policy — no copy-paste of PHI to unmanaged tools",
                ]
            ),
        ),
        (
            "Suggested 5-minute demo flow",
            b.join(
                [
                    "1) Show the one-slide architecture: Volume \u2192 DLT \u2192 Gold \u2192 Lakeview + App",
                    "2) Run or show a recent successful pipeline or job",
                    "3) Lakeview: one filtered chart and one reference KPI",
                    "4) App: one page backed by Lakebase (clinical or status)",
                    "5) Optional: Genie with 2 vetted questions \u2014 or skip if space is not ready",
                ]
            ),
        ),
        (
            "The outcome (why this pattern)",
            b.join(
                [
                    "Faster time from HL7 to dashboards without fragile manual parsing",
                    "Forecasts and accuracy next to operations, not a separate silo",
                    "Platform teams: repeatable delivery \u00b7 Business: clear metrics",
                ]
            ),
        ),
        (
            "Next steps in your workshop",
            b.join(
                [
                    "Import HL7_Streaming_E2E_Walkthrough.pptx into Google Slides (File \u2192 Import slides)",
                    "Walk `README.md` and `docs/ARCHITECTURE.md` in the repo for depth",
                    "Tie each slide to a live object in your Databricks workspace",
                ]
            ),
        ),
        (
            "Thank you",
            "HL7 streaming pipeline repository"
            "\n`README.md` \u00b7 `docs/SOLUTION_BRIEF_HL7_STREAMING.md` \u00b7 `docs/EVALUATOR_CHEAT_SHEET.md`"
            "\n`dashboards/adt_ens_bronze.lvdash.json`",
        ),
    ]


def build() -> Path:
    if not TEMPLATE.is_file():
        print(f"Missing template: {TEMPLATE}", file=sys.stderr)
        sys.exit(1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    specs = _slide_specs()
    n = len(specs)

    with tempfile.TemporaryDirectory() as tmp:
        tdir = Path(tmp)
        with zipfile.ZipFile(TEMPLATE, "r") as zf:
            zf.extractall(tdir)

        slides_dir = tdir / "ppt" / "slides"
        rels_dir = slides_dir / "_rels"
        for idx in range(2, n + 1):
            shutil.copy2(slides_dir / "slide1.xml", slides_dir / f"slide{idx}.xml")
            shutil.copy2(rels_dir / "slide1.xml.rels", rels_dir / f"slide{idx}.xml.rels")
            _next_creation_id(slides_dir / f"slide{idx}.xml")

        for idx in range(1, n + 1):
            t, s = specs[idx - 1]
            _set_slide_texts(slides_dir / f"slide{idx}.xml", t, s)

        pres_rels = (tdir / "ppt" / "_rels" / "presentation.xml.rels").read_text(encoding="utf-8")
        ct = (tdir / "[Content_Types].xml").read_text(encoding="utf-8")
        pres = (tdir / "ppt" / "presentation.xml").read_text(encoding="utf-8")

        max_rid = _max_r_id(pres_rels)
        for idx in range(2, n + 1):
            r_id = max_rid + (idx - 1)
            pres_rels = _append_slide_rels(pres_rels, idx, r_id)
            ct = _append_content_types(ct, idx)
            sld_id = 255 + idx
            pres = _append_sld_ids(pres, idx, sld_id, r_id)

        (tdir / "ppt" / "_rels" / "presentation.xml.rels").write_text(pres_rels, encoding="utf-8")
        (tdir / "[Content_Types].xml").write_text(ct, encoding="utf-8")
        (tdir / "ppt" / "presentation.xml").write_text(pres, encoding="utf-8")

        with zipfile.ZipFile(OUT_PATH, "w", zipfile.ZIP_DEFLATED) as zout:
            for path in sorted(tdir.rglob("*")):
                if path.is_file():
                    zout.write(path, path.relative_to(tdir).as_posix())

    return OUT_PATH


if __name__ == "__main__":
    out = build()
    print(f"Wrote {out}")
