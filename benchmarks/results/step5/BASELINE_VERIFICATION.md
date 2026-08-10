# Step5 Baseline Verification

Verified on: 2026-03-05
Source baseline: `benchmarks/results/step5/SUMMARY.md`

## Cross-check against target values

| Scenario | SUMMARY.md value (`lazy_savings_vs_eager`) | Plan target | Status |
|---|---:|---:|---|
| write-heavy | 0.6374 (63.74%) | 63.74% | match |
| parallel-editing | 0.6397 (63.97%) | 63.97% | match |
| large-artifact-reasoning | 0.2558 (25.58%) | 25.58% | match |
| access-always-read | 0.4714 (47.14%) | 47.14% | match |
| read-heavy | 0.0000 | expected near 0.00 | expected |
| access-pointer | 0.0000 | expected near 0.00 | expected |

## Notes

- The committed step5 baseline currently benchmarks `eager` and `lazy` only.
- `lease` and `access_count` are implemented/tested, but not included in step5 baseline outputs.
- `manifest.json` includes a checksum for `SUMMARY.md`:
  - `sha256:cfe198ccb0b03aa7cdb399810dd94e627b90f021b2eaa2ea7da7ff9deaca946e`
