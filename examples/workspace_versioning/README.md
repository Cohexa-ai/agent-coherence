# Workspace Versioning & Restore demo

Checkpoint a workspace whose members live in different backends — a file on disk,
S3 objects, a declared forward-only action surface — then let a failed attempt and
a foreign writer trash it, and bring it back with **per-member honesty** about
what can and cannot come back.

```
python -m examples.workspace_versioning.main             # the guarded demo
python -m examples.workspace_versioning.main --baseline  # loss-first, then guarded
```

Offline, deterministic, no API keys — a local in-process coordinator service over
a temporary SQLite registry plus the deterministic local S3 fake
(`ccs.testing.s3_local.LocalS3Client`). Exit code `0` iff the contract holds:
the guarded arm restores the restorable members and reports the absorbing
outcomes honestly; with `--baseline` the unprotected arm must FIRST demonstrate
the loss (no version history, no pointer — the original bytes are unrecoverable),
so the guarantee is measured against its absence rather than asserted.

## What it shows

The agent checkpoints the workspace (`checkpoint("before-migration")` — a
skew-declared cut with GC pin legs on by default), then the interference lands:
the file and one S3 object are overwritten, a stray object appears where the
manifest recorded ABSENT, and a foreign writer keeps racing a third object. One
`restore(checkpoint_id)` drives one conditional leg per member to a terminal
outcome:

| member | leg | outcome |
|---|---|---|
| `ws/notes.md` (file) | detection-guarded version-CAS (**no-arbiter**) | `restored` |
| `s3://reports/summary.txt` | native If-Match CAS | `restored` |
| `s3://scratch/leftover.txt` (ABSENT at capture) | delete leg (delete marker; history survives) | `restored` + `deleted_at_restore` |
| `s3://data/feed.txt` (sustained foreign writer) | bounded re-drive, budget exhausts | `conflict` — the foreign writer's state survives |
| `actions/deploy-step` (forward-only) | enumerated, skipped | `forward_only_skipped` |

The restore always **concludes** with the frozen per-member report — absorbing
outcomes are report content, never silent partial success and never a livelock.

```python
from ccs.adapters.workspace import WorkspaceVersioner

wv = WorkspaceVersioner(service=service, owner=owner, file_resolver=resolver)
wv.add_file_member(source, "ws/notes.md")
wv.add_object_member(binding, "reports/summary.txt")
wv.add_forward_only_member("actions/deploy-step")

checkpoint = wv.checkpoint("before-migration")   # pins on by default
report = wv.restore(checkpoint.record.checkpoint_id)
for member in report.members:
    print(member.member_path, member.outcome)    # per-member terminal truth
```

The same surface is scriptable from the shell via the `agent-coherence-workspace`
CLI (`checkpoint` / `list` / `status` / `restore` for file + forward-only
members; S3 members ride the Python API shown here, since their bindings carry
credentials).

## Honest scope

- **Demo pins caveat.** The S3 legal holds in this demo land on the *local
  deterministic S3 fake* (real Object Lock semantics are exercised by the
  `real_substrate`-marked suite against a real versioned bucket). File-member
  pins are **verification-only** over coordinator retention: retention is a
  bounded K/T policy with **no per-version hold** in v1, so a file member's tier
  stays `restorable-unpinned` and an expired retention window surfaces as
  `target_lost` at restore time — a pin there is a verification, not a
  guarantee.
- **File members are `no-arbiter`.** The file restore leg is a version-checked
  CAS whose foreign-edit signal is adapter-local *detection only* — it is never
  presented as substrate arbitration. Only the S3 legs ride a substrate that
  arbitrates natively (If-Match).
- **Single-host extras.** The checkpoint manifest, pin bookkeeping, restore
  progress, and the file-member version ledger live in a single-host
  coordinator. They add crash-resumable restore progress and honest tiering on
  one host — they are not a distributed store.
- **Cross-host carve-out.** Nothing here makes cross-host consistency claims. A
  foreign writer on another host is arbitrated only where the substrate itself
  arbitrates (the S3 If-Match legs); file members never are — their conflicts
  are detected and reported, full stop.

Comparing notes on multi-agent coherence?
https://github.com/Cohexa-ai/agent-coherence/discussions
