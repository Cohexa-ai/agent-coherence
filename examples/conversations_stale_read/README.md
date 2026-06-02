# Conversations stale-read demo

Two agents share one conversation. One caches it, the other revises it, and the
first acts on a stale copy. `CoherenceAdapterCore` invalidates the stale cache
so the reader re-fetches before it acts.

```bash
python -m examples.conversations_stale_read.main
```

No API keys, no cost, deterministic.

## What this actually shows (and what it does not)

The honest framing, after measuring the providers:

- **The Conversations APIs are not the bug.** A consistency probe (`probe.py`)
  ran 100 OpenAI + 20 Mistral write-then-cross-client-read trials and observed
  **zero** stale reads — both APIs commit a write before returning the ACK, so a
  separate client's read after that ACK sees it. (This *fails to falsify* server
  consistency under load/concurrency; it does not prove it.)
- **The bug is the client cache.** Agents cache conversation state locally to
  avoid re-fetching and re-paying for the whole history. That local cache goes
  stale the moment a peer writes — regardless of how consistent the server is.
- **Coherence is about the readers.** `CoherenceAdapterCore` tracks who holds
  what and invalidates a reader's cache when a peer writes, so the reader takes a
  cache miss and re-fetches instead of acting on stale state.

This is server-consistency-independent: the same mechanism protects a Session
cache, a `conversation_id` cache, or any shared artifact.

Two claims we deliberately do **not** make: that OpenAI/Mistral Conversations
"serve stale reads" (they did not, in our trials), and that `previous_response_id`
is deprecated (it is not, as of 2026-05).

## Files

| File | Role |
|---|---|
| `broken.py` | No coherence — agent B acts on a stale local cache over a consistent store |
| `fixed.py` | `CoherenceAdapterCore` invalidates B's cache; B re-fetches the fresh version |
| `main.py` | Runs both side by side and prints the divergence |
| `probe.py` | Q6 consistency probe — measures each vendor's read-after-write behavior |

## Re-running the Q6 probe (optional, needs keys)

The probe makes live calls. Inject credentials with 1Password so keys never
touch disk or the command line:

```bash
pip install -e ".[openai,mistral]"
cp .env.op.example .env.op   # edit the op:// references to your items
scripts/run-q6-probe.sh --vendor both --trials 100
```

The probe is rate-limit aware (`--delay-ms`) and writes a machine-readable
verdict (git-ignored) containing only hashes and counts — never credentials or
conversation content.
