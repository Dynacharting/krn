# SPEC: Live-socket WebSocket E2E test (Boost.Beast)

**Slug:** ws-e2e
**Date:** 2026-05-01
**Status:** Approved
**Author:** ndrandal
**Repo:** krn

## Problem

`WebSocketServer` + `ClientSession` are the production WS handler — every external client lands on this code path — and they're structurally untested today. The existing WS coverage is `WsBridgeTest` / `WsResponderTest`, both of which use the in-process bridge stub. That leaves the actual Beast accept loop, handshake, frame handling, back-pressure, and graceful close uncovered. Two ADRs explicitly defer this work: ADR-001 (chose the bridge for Phase 1) and ADR-004 (legacy-`symbol` back-compat untested at the WS layer; only engine-level via `NoSymbolPipelineTest.LegacySymbolAtTopLevel_StillBuilds`). The audit closes both in one move with a single Boost.Beast E2E test.

## Proposed change

Add a live-socket test under `tests/ws/WebSocketE2ETest.cpp` that boots a real `WebSocketServer` on `127.0.0.1:0` (OS-assigned port), dials it with a Boost.Beast WS client, runs three smoke scenarios (connect-and-close, build a trivial pipeline + receive an event, send a legacy `symbol`-keyed request and verify it's accepted), and tears down cleanly. ~80 LOC of plumbing in one test file. No production code changes.

## Scope

- New `tests/ws/WebSocketE2ETest.cpp` containing:
  - **Fixture** that starts a `WebSocketServer` on `127.0.0.1:0` in a background `std::thread` running `io_context::run`, captures the actual bound port, and tears the server down deterministically in `TearDown`.
  - **`ConnectAndClose`** — open a Beast WS connection, send a clean close, assert no exceptions and the server's session count returns to zero within a short bounded wait.
  - **`PipelineRoundTrip`** — send a minimal valid request tree (a `Listener` on a synthetic field), inject a synthetic event via the engine's `Dispatcher`, read the WS response frame, assert the value matches.
  - **`LegacySymbolAlias`** — send a request using the deprecated `"symbol"` key at top level (instead of `"streamKey"`), assert the server accepts it and the resulting pipeline behaves identically to the canonical form. This is the explicit ADR-004 closer.
- Use Boost.Beast WS client APIs already linked into the engine (no new third-party deps).
- Each test scenario uses an independent `io_context` for the client to avoid coupling between fixtures.
- Wire the test under the existing `krn_tests` GLOB; no new CMake target.

## Non-goals

- **Not testing rate limits, MAX_SUBSCRIPTIONS, length validation, or cancel error paths.** Those are ENC-48 (D4)'s scope; this test is the structural smoke.
- **Not exercising TLS / WSS.** Plain WS over loopback only.
- **Not exercising the binary frame path.** Text frames only — that's all `ClientSession` accepts today.
- **Not asserting timing** beyond loose timeouts (each scenario carries a 2-second hard cap before failing).
- **Not touching production code.** If a scenario fails because of a real bug, the bug is fixed in a separate ticket; this proposal just adds the test.
- **Not running in CI on Windows.** Linux/macOS only — Beast on Windows has historically been flaky on this codebase.

## Acceptance criteria

1. `tests/ws/WebSocketE2ETest.cpp` exists with three named test cases: `ConnectAndClose`, `PipelineRoundTrip`, `LegacySymbolAlias`.
2. `LegacySymbolAlias` sends a payload containing `"symbol":"…"` at top level (no `"streamKey"`), the server accepts it, and the resulting pipeline emits the expected event. Closes the ADR-004 gap; cite ADR-004 in the test docstring.
3. All three tests pass under `ctest --output-on-failure` on Linux. Each scenario completes in under 2 seconds.
4. The fixture binds `127.0.0.1:0`, captures the OS-assigned port, and the test does not depend on any well-known port.
5. The fixture leaves no socket in `TIME_WAIT` longer than necessary — the WS server's accept loop is `cancel`/`stop`'d in `TearDown` and the io_context's thread is `join`'d.
6. No new third-party dependency; only headers already in `Boost::system`.
7. Repeated runs (`ctest --repeat-until-fail 5`) stay green — the test is not flaky.

## Constraints

- **Performance:** test runs in <2s per case; total suite increase <10s.
- **Compatibility:** loopback only; no host or hardware assumptions beyond a working IPv4 stack.
- **Dependencies:** none beyond Boost.Beast (already linked).
- **Deadline:** none.

## Affected systems / callers

- New: `tests/ws/WebSocketE2ETest.cpp`.
- No changes to `WebSocketServer`, `ClientSession`, or their headers.
- No CMake changes; the existing `tests/**/*.cpp` GLOB picks up the new file.
- ADR-001 (`docs/`) — annotate "closed by ENC-47" once the test lands.
- ADR-004 — same annotation.

## Alternatives considered

- **Use the existing in-process bridge for "E2E" coverage.** Rejected: defeats the purpose. ADR-001 explicitly deferred the live-socket case; we're closing that deferral.
- **Run the test against the running `krn_server` binary as a black-box.** Rejected: too much CI plumbing for the value; in-process Beast is sufficient and far easier to debug.
- **Skip the legacy-symbol case and let ENC-50 close ADR-004.** Rejected: ADR-004 is currently the AC6 gap; closing it now means ENC-50 can drop the alias safely later (it has end-to-end coverage to point at).

## Risks

- **Test flakiness from thread + io_context shutdown ordering.** Mitigation: a single shared `io_context` for the server, run in one dedicated thread, drained in `TearDown` via `io_context::stop()` then `thread::join()`. The pattern is well-known and used in other `tests/`.
- **Port bind race on parallel test runs.** Mitigation: `127.0.0.1:0` everywhere; no fixed ports.
- **CI environments without IPv4 loopback.** Mitigation: GTest's `GTEST_SKIP()` if `bind` fails with `EADDRNOTAVAIL`.

## Open questions

- None.
