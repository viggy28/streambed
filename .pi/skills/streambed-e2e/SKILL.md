---
name: streambed-e2e
description: Proves that a Streambed change works through its public boundaries using the existing integration infrastructure. Use when asked to test, validate, or provide end-to-end proof for a branch, diff, PR, issue, or Streambed behavior.
compatibility: Requires Go, CGO, Docker with Compose, and the Streambed repository.
---

# Streambed E2E Proof

Test Streambed as a user would: through the compiled binary, PostgreSQL, the Postgres wire query server, and stored lakehouse data. Reuse the repository's existing integration tests, Docker Compose stack, and CI commands. Do not create a parallel test harness.

## Proof standard

A passing command is evidence, but it is not sufficient by itself. A successful proof must connect:

1. the claim made by the change;
2. an action performed through a public boundary;
3. an independently observable result;
4. the raw command output or artifact supporting that result.

Never claim success from source inspection, mocks, or unit tests alone. Never say a test passed if it was skipped, unavailable, flaky, or only partially exercised.

## Public boundaries

Prefer these boundaries, in order of relevance:

- the compiled `streambed` CLI;
- writes to source PostgreSQL through SQL;
- reads through Streambed's PostgreSQL wire endpoint;
- Parquet, Iceberg, or DuckLake state produced in object storage/catalogs;
- process lifecycle behavior such as restart, resume, shutdown, and failure recovery.

Internal Go APIs may help diagnose a failure, but they are not end-to-end proof.

## Workflow

### 1. Establish the test subject

From the repository root:

- inspect `git status --short`;
- identify the base branch, defaulting to `main`;
- inspect committed and uncommitted changes against the merge base;
- read the relevant issue, PR description, specification, and existing tests when available;
- state the user-visible or operator-visible claims before running tests.

Do not discard, overwrite, or clean unrelated working-tree changes.

Produce a small claim table:

| Claim | Public action | Observable result | Planned evidence |
|---|---|---|---|

If no externally observable claim can be inferred, ask the user rather than inventing one.

### 2. Find the nearest existing coverage

Search `test/integration`, `test/querycompat`, and the CI workflow for related scenarios. Prefer extending or invoking existing coverage over writing a new harness.

Use the same environment as the repository:

```bash
./scripts/test-integration.sh
```

For focused investigation, use the commands and Compose file already present in `.github/workflows/ci.yml` and `scripts/test-integration.sh`. Keep cleanup guarded with a shell trap. Do not leave containers, volumes, background Streambed processes, replication slots, or temporary databases behind.

### 3. Check prerequisites honestly

Verify Go/CGO and Docker before testing. If Docker is unavailable, try to start or restore it when safe, then retry. If a required dependency still cannot run, report the exact blocked scenario and command; do not silently substitute a lower-level test.

Record:

- commit SHA and whether the working tree is dirty;
- operating system and architecture;
- `go version`;
- relevant Docker versions;
- exact commands executed.

### 4. Run deterministic regression coverage

At minimum:

1. build the real binary with `go build -o streambed ./cmd/streambed`;
2. run the narrowest relevant existing unit tests for fast feedback;
3. run the nearest existing integration or query-compatibility tests;
4. run the full `./scripts/test-integration.sh` when the change can affect CDC, storage formats, recovery, query behavior, or shared infrastructure.

Do not weaken assertions, remove race checks, or change production configuration merely to make a test pass.

### 5. Exercise the changed behavior end to end

Add one PR-specific black-box scenario when existing tests do not directly prove the claim. Drive it through public boundaries. Depending on the change, this normally means:

1. start PostgreSQL and MinIO with the existing integration Compose stack;
2. start the compiled Streambed binary with isolated names and temporary state;
3. create and mutate source data using SQL;
4. wait using a bounded observable condition, not an unexplained sleep;
5. query through Streambed or invoke the relevant CLI command;
6. compare expected and observed rows, metadata, files, or process behavior;
7. test the most important failure or recovery path, such as restart/resume, when relevant.

Temporary exploratory commands may live outside the repository. If the scenario catches a real regression or protects an important contract, recommend converting it into an existing Go integration or query-compatibility test rather than preserving a second shell-based suite.

### 6. Preserve evidence

Create a temporary evidence directory, normally:

```text
${TMPDIR:-/tmp}/streambed-e2e-proof/<commit-or-working-tree>/<timestamp>/
```

Capture relevant material without secrets:

- command transcript and exit statuses;
- Streambed process logs;
- source SQL and results;
- query-server SQL and results;
- relevant object/catalog listings;
- expected-versus-observed comparisons;
- environment metadata.

Redact credentials, tokens, and connection strings containing secrets. Evidence should be sufficient for another engineer to repeat the scenario.

### 7. Report the verdict

Finish with exactly these sections:

## Verdict

`PROVEN`, `NOT PROVEN`, or `BLOCKED`.

Use `PROVEN` only when every stated claim has direct end-to-end evidence. A unit or integration suite passing does not prove an uncovered claim.

## Claims and evidence

For each claim, report the action, expected result, observed result, and evidence path.

## Commands run

List exact commands and outcomes. Distinguish passed, failed, and skipped commands.

## Residual risks

List behavior not exercised, environmental differences from production, flaky observations, and assumptions. Say `None identified` only when justified.

## Regression recommendation

State whether the exploratory scenario should become part of `test/integration`, `test/querycompat`, or the existing CI selection. Do not add it automatically unless the user asked for code changes.

## Scope control

- Test the current change and one adjacent failure mode; do not attempt every Streambed feature.
- Prefer focused tests first, then broaden only when shared CDC/storage/query behavior is touched.
- Treat both Iceberg and DuckLake as separate observable targets when the changed contract promises parity.
- Keep CI as the enforcement layer. This skill supplies PR-specific judgment and evidence; it does not replace `.github/workflows/ci.yml`.
