# Capstone: Distributed KV Store with Erasure Coding & Raft Consensus

## Repository Layout

| Path | What's there |
|---|---|
| `capstone/project/lab/src/capstone/` | All source code (CoordinatorNode, RegionalNode, CapstoneClient, etc.) |
| `capstone/project/lab/tst/dslabs.capstone/` | Test suite (46 tests: run + search) |
| `capstone/project/lab/README.md` | Build instructions, test commands, architecture diagram |
| `capstone/Capstone Design Document.pdf` | Original design document |
| `capstone/design_additions.md` | Design for GC and Raft (features added after the original design doc) |
| `oral-exam-hooks/hooks.md` | Assumption and tradeoff hooks |
| `ai/LOG.md` | AI interaction log |

## Project Overview

A distributed key-value store built on the dslabs framework that prioritizes **cross-region confidentiality** and **consistency under partition**. Values are AES-128 encrypted, then split via Reed-Solomon erasure coding (k data + m parity fragments) and distributed across regional storage nodes. The AES decryption key is split via Shamir threshold secret sharing so no single region can reconstruct or decrypt the data alone.

Key features:
- **Reed-Solomon erasure coding** over GF(256) — tolerates m region failures
- **Shamir secret sharing** — threshold key reconstruction across regions
- **Challenge-response authentication** — HMAC-SHA256 handshake, per-session tokens
- **Per-key ownership** — first writer owns the key, others denied
- **Dynamic region membership** — regions join at runtime, parity (m) increases
- **Garbage collection** — old versions cleaned on commit
- **Raft coordinator replication** — 3 coordinators with leader election, log replication, strict reads, log compaction
- **Deterministic search tests** — BFS/DFS model checking alongside run tests

## Design Document Divergences

The original design document is at `capstone/Capstone Design Document.pdf`. Key divergences:

- **"Regional service rejects duplicate writes"** — changed to idempotent acks, required for reliability under message loss.
- **"AES key never stored at coordinator"** — temporarily stored during in-flight writes for retransmission to unacked regions.
- **"Fixed set of regions"** — implemented dynamic region join (add-only) with per-version encoding to preserve backward compatibility and mimic what might happen in a real system.
- **"Authentication via authenticated channels (TLS)"** — replaced with application-layer HMAC challenge-response; dslabs framework has no wire protocol for TLS.
- **"Coordinator replication is out of scope"** — implemented Raft consensus after TA feedback to eliminate the single point of failure.
- **"Buffer client requests on busy"** — evaluated and rejected; client retry on its own as not much value was found in unbounded buffer request queue.

Other decisions are somewhat detailed in the ai log and the oral-exam-hook log. But a lot of active decisions were made to find a reasonable intersection between each of the 5 rubric components. I aim to explore these further in the reflection. But for now, my main goal with this project was to create a consistent, partition tolerant, and secure store for valuable information. This required a lot of iteration, for example initially figuring out how to do erasure coding was a big learning, then further expanding it to encrypt information separately into each store to ensure they can't get partial info on their own, then further acting against client maliciousness by creating-per-key ownership and client per session auth. Additionally, working later to deal with memory issues such as unbounded garbage collection from stale versions. And eventually including RAFT to reduce the previous single point of failure the coordinator introduces. There is of course a lot more subtle things I could include, such as why our GC operates in the "fire-and-forget" mode rather than having regions coordinate their GC (or upkeep, somewhat like dp2). How all of these affected the consistency vs latency trade offs and so much more. But I'll leave those deliberations for the reflection.
