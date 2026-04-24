#!/usr/bin/env python3
"""
Build HL7_Streaming_Evaluation_Deck.pptx using only the Python standard library.

Uses the vendored OOXML shell from python-pptx's MIT-licensed test fixture:
  docs/presentations/vendor/python_pptx_test_template.pptx
(See docs/presentations/vendor/README.txt)

Optional: pip install python-pptx for an alternate builder — not required.

Usage:
  python3 scripts/build_evaluation_deck.py
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
OUT_PATH = OUT_DIR / "HL7_Streaming_Evaluation_Deck.pptx"

# Title slide layout: center title + subtitle (from test.pptx).
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
    """Give duplicated slides a distinct p14:creationId so PowerPoint stays happy."""
    raw = slide_path.read_text(encoding="utf-8")
    raw2 = re.sub(
        r'(<p14:creationId[^>]*val=")(\d+)(")',
        lambda m: f'{m.group(1)}{int(m.group(2)) ^ 0x9E3779B9 & 0xFFFFFFFF}{m.group(3)}',
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
    """Insert <p:sldId id="..." r:id="rId..."/> before closing sldIdLst."""
    close = "</p:sldIdLst>"
    insert = f'<p:sldId id="{sld_id}" r:id="rId{r_id}"/>'
    if insert in pres_text:
        return pres_text
    return pres_text.replace(close, insert + close)


def _slide_specs() -> list[tuple[str, str]]:
    bullet = " \u2022 "
    return [
        (
            "HL7 ED & ICU Operations",
            "Lakehouse \u00b7 Lakebase \u00b7 Databricks Apps \u00b7 Genie"
            "\nEvaluation / demo deck \u2014 HL7 Streaming pipeline",
        ),
        (
            "Agenda",
            bullet.join(
                [
                    "Business use case (ED & ICU)",
                    "End-to-end architecture & data flow",
                    "Lakebase & reverse ETL",
                    "Genie, App UX, Security, DAB",
                    "Engineering quality, performance, demo script",
                    "Rubric map (100 pts)",
                ]
            ),
        ),
        (
            "Business use case",
            bullet.join(
                [
                    "Operational analytics for ED and ICU",
                    "Census, arrivals/discharges, trends, ML forecasts, throughput",
                    "Clinical personas: situational awareness without Spark SQL",
                    "Platform personas: pipeline health, jobs, freshness (SLO-style)",
                ]
            ),
        ),
        (
            "End-to-end architecture (20 pts)",
            bullet.join(
                [
                    "HL7 \u2192 UC volume (landing)",
                    "DLT bronze \u2192 silver \u2192 gold (medallion)",
                    "ML: AutoML + batch inference on gold features",
                    "Lakebase Postgres: read-optimized SQL for the app",
                    "Apps (Streamlit) + Genie (UC / warehouse)",
                    "Clinical (Lakebase) vs Platform (workspace APIs) by design",
                ]
            ),
        ),
        (
            "Logical layers",
            bullet.join(
                [
                    "L1 Landing \u00b7 L2 DLT bronze/silver \u00b7 L3 Gold in UC",
                    "L4 Features & inference (jobs)",
                    "L5 Lakebase read model \u00b7 L6 Apps & Genie",
                    "See docs/ARCHITECTURE.md",
                ]
            ),
        ),
        (
            "Lakebase usage (15 pts)",
            bullet.join(
                [
                    "OAuth via Databricks Apps identity (no static DB password in Git)",
                    "Low-latency dashboard SQL on gold via Postgres API",
                    "db.py: token cache, lock, batch queries; Load test page",
                    "Grants: notebooks/11_lakebase_grants.py",
                    "Frame as operational analytics read model",
                ]
            ),
        ),
        (
            "Reverse ETL (10 pts)",
            bullet.join(
                [
                    "Gold (UC) \u2192 Lakebase: governed, job-scheduled",
                    "09_lakebase_sync.py: CDF + synced-table prep (Autoscaling UI path)",
                    "10_lakebase_load.py: Spark read gold, TRUNCATE + INSERT snapshot",
                    "Order: DLT \u2192 inference (if used) \u2192 Lakebase (bundle workflow)",
                    "State which path is primary in your workspace",
                ]
            ),
        ),
        (
            "Genie integration (10 pts)",
            bullet.join(
                [
                    "NL \u2192 SQL over curated Genie space (UC tables)",
                    "pages/8_genie_chat.py + utils/genie_client.py",
                    "GENIE_SPACE_ID in app.yaml; 12_genie_uc_grants.py",
                    "Runtime: app service principal (document vs end-user)",
                    "Demo: 2\u20133 canned questions that succeed in the space",
                ]
            ),
        ),
        (
            "Databricks App quality (10 pts)",
            bullet.join(
                [
                    "Multipage Streamlit: theme, navigation, home IA",
                    "Clinical \u00b7 Platform \u00b7 Genie groupings",
                    "Interactivity: filters, refresh, jobs, DLT live, Lakebase load test",
                    "Optional: hl7-appkit-app (AppKit) as second framework story",
                ]
            ),
        ),
        (
            "Security & governance (10 pts)",
            bullet.join(
                [
                    "UC: USE + SELECT on gold for automation + app SP",
                    "Lakebase: databricks_create_role + Postgres SELECT aligned to UC",
                    "Genie: space-bound tables + SQL warehouse CAN USE",
                    "Secrets in scopes (not Git) for integrations",
                    "docs/ARCHITECTURE.md \u2014 Security and identity",
                ]
            ),
        ),
        (
            "CI/CD & DAB (10 pts)",
            bullet.join(
                [
                    "databricks.yml + resources/*.yml",
                    "Targets dev / staging / prod with variables",
                    "Reproducible jobs & pipelines; same bundle across envs",
                    "scripts/run_full_stack_full_refresh.sh",
                    "State manual bundle deploy vs CI",
                ]
            ),
        ),
        (
            "Data engineering & performance (5 + 5)",
            bullet.join(
                [
                    "DE: resources/hl7_pipeline.yml + HL7 modeling story",
                    "Trace one entity (e.g. ADT \u2192 census) through bronze \u2192 gold",
                    "Perf: short-lived connections, lock, run_query_batch",
                    "Lakebase load test; Photon / sizing in bundle where set",
                ]
            ),
        ),
        (
            "Vibe coding (10 pts)",
            bullet.join(
                [
                    "Not in Git: 2\u20133 concrete examples (Streamlit, SQL, YAML, Genie)",
                    "Name tools (Claude, Cursor, Genie-assisted SQL, docs)",
                    "Tie each example to a shipped file or notebook",
                ]
            ),
        ),
        (
            "Rubric at a glance (100 pts)",
            bullet.join(
                [
                    "Architecture 20 \u00b7 Lakebase 15 \u00b7 Vibe coding 10",
                    "Reverse ETL 10 \u00b7 Genie 10 \u00b7 App 10 \u00b7 Security 10 \u00b7 DAB 10",
                    "DE 5 \u00b7 Performance 5 \u00b7 Presentation 5",
                    "Full map: docs/EVALUATOR_CHEAT_SHEET.md",
                ]
            ),
        ),
        (
            "5-minute demo script",
            bullet.join(
                [
                    "1 Diagram: ingest \u2192 DLT \u2192 gold \u2192 Lakebase \u2192 App + Genie",
                    "2 One clinical page (Lakebase)",
                    "3 One platform view (status, jobs, or DLT live)",
                    "4 Genie: two curated questions (or space + grants)",
                    "5 Close: UC + SP + OAuth; daily runbook / workflow job",
                ]
            ),
        ),
        (
            "Thank you",
            "Repository: HL7 Streaming"
            "\nREADME.md \u00b7 docs/ARCHITECTURE.md \u00b7 docs/EVALUATOR_CHEAT_SHEET.md",
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
        # Duplicate from pristine slide1 before mutating placeholders
        for idx in range(2, n + 1):
            shutil.copy2(slides_dir / "slide1.xml", slides_dir / f"slide{idx}.xml")
            shutil.copy2(rels_dir / "slide1.xml.rels", rels_dir / f"slide{idx}.xml.rels")
            _next_creation_id(slides_dir / f"slide{idx}.xml")

        for idx in range(1, n + 1):
            t, s = specs[idx - 1]
            # ElementTree escapes text on write — do not pre-escape &.
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
                    arc = path.relative_to(tdir).as_posix()
                    zout.write(path, arc)

    return OUT_PATH


if __name__ == "__main__":
    out = build()
    print(f"Wrote {out}")
