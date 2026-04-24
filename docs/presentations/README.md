# Presentation assets

| File | Description |
|------|-------------|
| `HL7_Streaming_Evaluation_Deck.pptx` | **Evaluation / demo deck** aligned with `docs/EVALUATOR_CHEAT_SHEET.md` (regenerate with the script below). |
| `vendor/python_pptx_test_template.pptx` | MIT-licensed OOXML shell from [python-pptx tests](https://github.com/scanny/python-pptx/tree/v0.6.23/tests/test_files) — see `vendor/README.txt`. |

## Regenerate the PowerPoint

From the repo root (stdlib only — no `pip install`):

```bash
python3 scripts/build_evaluation_deck.py
```

Optional: after `pip install python-pptx`, you can replace the builder with a `python-pptx`-based script for richer layouts; the current script is designed for offline/air-gapped environments.
