# Presentation assets

**Google Slides (live deck):** [HL7 Streaming — Reference slide](https://docs.google.com/presentation/d/1ViCSD922hbpnIbp-dM9VsLENJiiCVy2nbAK44sjhQSs/edit?slide=id.g3db67b81f56_0_0#slide=id.g3db67b81f56_0_0) — repo cannot edit Slides; copy from **`REFERENCE_WORKSPACE_LINKS.md`** below.

| File | Description |
|------|-------------|
| `REFERENCE_WORKSPACE_LINKS.md` | **Jobs / pipeline / alerts URLs** for e2-demo-field-eng dev — paste into Slides or speaker notes. |
| `HL7_Streaming_Styled_Deck.pptx` | Styled from `~/Downloads/LLMClarity_Presentation.pptx` layout (same visual flow style, HL7 Streaming content). Regenerate with `python3 scripts/build_hl7_styled_from_llmclarity.py`. |
| `HL7_Streaming_E2E_Walkthrough.pptx` | **Step-by-step** story: ingest → DLT / medallion → Lakeview / App / Lakebase, plus a suggested live demo. Regenerate with `python3 scripts/build_e2e_presentation_deck.py`. |
| `HL7_Streaming_Evaluation_Deck.pptx` | **Evaluation / demo deck** aligned with `docs/EVALUATOR_CHEAT_SHEET.md` (regenerate with the script below). |
| `vendor/python_pptx_test_template.pptx` | MIT-licensed OOXML shell from [python-pptx tests](https://github.com/scanny/python-pptx/tree/v0.6.23/tests/test_files) — see `vendor/README.txt`. |

## Regenerate the PowerPoint

From the repo root (stdlib only — no `pip install`):

```bash
python3 scripts/build_hl7_styled_from_llmclarity.py
python3 scripts/build_e2e_presentation_deck.py
python3 scripts/build_evaluation_deck.py
```

**Use in Google Slides:** in your deck, use **File → Import slides** and upload the `.pptx` (Google cannot be edited from this repository).

Optional: after `pip install python-pptx`, you can replace the builder with a `python-pptx`-based script for richer layouts; the current script is designed for offline/air-gapped environments.
