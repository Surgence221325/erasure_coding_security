# Capstone: Distributed KV Store with Erasure Coding

A distributed key-value store built on the dslabs framework. Uses Reed-Solomon erasure coding, Shamir secret sharing, AES-128 encryption, challenge-response authentication, per-key ownership, and dynamic region membership.

## Requirements

- Java 17 (pinned via `gradle.properties`)
- Python 3 (for test runner)

## Build

```bash
cd capstone/project
./gradlew assemble
```

## Run Tests

```bash
# All tests (run + search):
cd capstone/project/build/handout
python3 run-tests.py --lab capstone

# Run tests only (faster, ~15s):
python3 run-tests.py --lab capstone --no-search

# Search tests only (~2min):
python3 run-tests.py --lab capstone --no-run
```

## One-Liner

```bash
cd capstone/project && ./gradlew assemble && cd build/handout && python3 run-tests.py --lab capstone --no-search
```

## Test Suite

32 tests across 8 categories: basic correctness, fault tolerance, multi-client, stress, authentication/authorization, integrity/corruption, dynamic membership, and deterministic search (BFS/DFS model checking).
