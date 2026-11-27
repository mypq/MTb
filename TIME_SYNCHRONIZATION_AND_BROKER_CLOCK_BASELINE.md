# GOLD-BOT — Time Synchronization & Broker Clock Baseline

**Status:** Active engineering handover  
**Last verified:** 2026-11-27 (local verification around 05:43, UTC+03:00)  
**Repository:** mypq/MTb  
**Branch:** main  
**Scope:** Windows + WSL2 Ubuntu + Chrony/NTS + GOLD-BOT time verification + MT5 broker-clock evidence

This document is the accepted handover reference for developers and future assistants. It records the current design, dependencies, decisions, evidence, safety boundaries, and next work. Always inspect the current source before changing anything; update this document when accepted behavior or decisions change.

---

## 1. Current status at a glance

**The WSL/Chrony query-routing problem is resolved in the current GOLD-BOT production path and passed the latest local stability check.**

Root cause: WSL can expose another Chrony daemon in its root namespace. Chrony command requests sent to UDP localhost port 323 could be answered by either daemon. GOLD-BOT therefore sometimes saw Ubuntu NTS sources and sometimes the WSL PHC0 clock, even when Ubuntu's service process appeared stable.

Accepted fix: GOLD-BOT explicitly queries Ubuntu Chrony's protected Unix-domain socket:

    /run/chrony/chronyd.sock

All three queries are executed through one WSL invocation using the explicit socket and non-interactive restricted sudo:

1. tracking
2. sources -v
3. authdata

Strict three-provider quorum remains enabled. No provider was removed, no quorum requirement was weakened, PHC0 was not deleted, and Hyper-V time synchronization was not changed.

### Latest evidence

- Direct socket-pinned Ubuntu Chrony queries succeeded.
- GOLD-BOT ChronyStatusService snapshot returned synchronized=True, quorum_met=True, healthy_sources=3, nts_authenticated_sources=3, error=None.
- Consecutive local test: **10/10 PASS**, two seconds between snapshots. Every result had sync=True, quorum=True, three healthy sources, three authenticated provider identities, selected source ntppool1.time.nl, and no error.
- GitHub Actions workflow run **#925** on commit **2ab6625b757323a67bf9f684831ea033ab039966** completed successfully.
- A previous comprehensive local test run reported 63 passed. A focused local run reported 62 passed; these were different runs/scopes and must not be added together.
- CI success proves the automated checks passed, not live NTS connectivity. The 10/10 test proves stability during that short local test window, not permanent uptime.

---

## 2. System topology

    Windows host
      |
      +-- GOLD-BOT Python process
            |
            +-- ChronyStatusService
                  |
                  +-- wsl.exe --exec /bin/bash -c "<combined query>"
                        |
                        +-- sudo -n /usr/bin/chronyc
                              -h /run/chrony/chronyd.sock
                              |
                              +-- Ubuntu Chrony daemon
                                    +-- time.cloudflare.com (NTS)
                                    +-- nts.netnod.se (NTS)
                                    +-- ntppool1.time.nl (NTS)

**Critical:** Do not route production Chrony queries through UDP localhost port 323. In WSL that endpoint is ambiguous and may reach the other daemon. The Ubuntu Unix socket is the deliberate endpoint.

External dependencies:
- Windows with WSL2.
- WSL distribution named Ubuntu.
- Ubuntu systemd and chrony service.
- chronyc at /usr/bin/chronyc.
- Ubuntu socket at /run/chrony/chronyd.sock.
- Exact restricted sudoers entries described below.
- DNS/network access to the three configured NTS providers.
- Python environment and project dependencies on Windows.

If the distribution name, socket path, executable path, sudoers arguments, or provider names change, update implementation, tests, and this document together.

---

## 3. Accepted Ubuntu Chrony configuration

File: /etc/chrony/conf.d/goldbot-nts.conf

    port 0
    server time.cloudflare.com iburst nts
    server nts.netnod.se iburst nts
    server ntppool1.time.nl iburst nts
    minsources 3

The Ubuntu main configuration has active confdir /etc/chrony/conf.d.

Policy:
- Three NTS providers are required.
- minsources 3 remains enabled.
- Do not delete PHC0 or other WSL clock infrastructure as a workaround.
- Do not weaken quorum to one or two sources.
- Do not expose Chrony command access to the LAN.
- Do not widen socket/network permissions casually.
- Do not disable Hyper-V/WSL time synchronization as an experiment.

The main chrony.conf had sourcedir /etc/chrony/sources.d commented. Do not assume entries in that directory are active.

### NTS authdata interpretation

chronyc authdata reports NTS authentication records and cookie state. Existing cookies/records alone do not prove a fresh NTS handshake at the exact instant of inspection. Evaluate authdata together with current selectable sources, reachability, tracking, and the application's quorum result.

---

## 4. Restricted sudoers integration

File: /etc/sudoers.d/goldbot-chrony

The approved socket-pinned entries are:

    ali ALL=(root) NOPASSWD: /usr/bin/chronyc -h /run/chrony/chronyd.sock -n tracking, /usr/bin/chronyc -h /run/chrony/chronyd.sock sources -v, /usr/bin/chronyc -h /run/chrony/chronyd.sock authdata

An older restricted entry may also exist in this file. Preserve it unless its removal is separately reviewed and approved.

Rules:
- Executable path and argument order must match the sudoers entry exactly.
- GOLD-BOT uses sudo -n; it must fail rather than prompt for a password.
- Do not grant unrestricted passwordless sudo.
- After sudoers edits, validate with sudo visudo -cf /etc/sudoers. Interactive authentication may be required.
- The socket directory is protected; root execution is intentional.

This sudoers file is external machine configuration. It is not automatically installed by this repository. New machines require deliberate, secure provisioning.

---

## 5. GOLD-BOT code design

### Chrony status

Primary file: backend/market_data/chrony.py

On the default Windows configuration, ChronyStatusService uses _run_windows_bundle(). This method:
- runs one WSL Bash invocation for all three queries;
- invokes sudo -n /usr/bin/chronyc with -h /run/chrony/chronyd.sock;
- runs tracking, sources -v, and authdata;
- separates outputs using explicit markers;
- preserves source hostnames in sources output.

**Do not add -n to the sources command.** Numeric-only output removes provider hostnames needed by the application's provider-identity parser.

The source parser counts selectable healthy sources with valid stratum and nonzero reach. The authdata parser identifies the approved NTS providers. The quorum requires three healthy sources, three authenticated provider identities, and exact identity-set agreement. Synchronization/tracking validity is also required for a healthy status.

Provider identities:
- Cloudflare: time.cloudflare.com
- Netnod: hostnames under nts.netnod.se
- time.nl: ntppool1.time.nl

Do not change the policy without an explicit architecture decision and tests for missing, stale, unhealthy, and mismatched providers.

### WSL keepalive

Primary file: backend/market_data/wsl_keepalive.py

GOLD-BOT owns a WSL keepalive process during application lifespan. It launches an attached WSL process, validates its marker/PID, checks Chrony readiness, supervises retries/health, and stops only the process it created. It must not terminate the whole WSL distribution or kill unrelated processes.

Integration is in backend/main.py:
- startup attempts to start the keepalive;
- a supervisor task monitors and retries;
- shutdown signals the supervisor and awaits cleanup before database disposal.

Do not create a separate always-on service without explicit design review. The accepted design is lifecycle ownership by GOLD-BOT.

**Implementation review note:** chrony.py also contains helper/fallback query methods used outside the default Windows bundle path (for example, explicit-command/test configurations). Before expanding or reusing those paths in Windows production, inspect them and ensure they cannot silently fall back to ambiguous UDP. The verified production default path is the socket-pinned bundle.

---

## 6. Accepted decisions — do not regress

1. Production Windows Chrony evidence must use Ubuntu's /run/chrony/chronyd.sock, never ambiguous UDP localhost.
2. Collect tracking, sources, and authdata in one WSL invocation.
3. Preserve source hostnames; sources command must not use -n.
4. Keep strict three-provider NTS quorum.
5. Do not remove PHC0 as a workaround.
6. Use exact restricted sudo commands with sudo -n.
7. Manage WSL keepalive through GOLD-BOT lifespan.
8. Time infrastructure work does not authorize enabling trading or placing orders.
9. Distinguish unit tests, CI, and live host evidence in every report.
10. Never fabricate or manually edit BrokerOffsetRegistry evidence; use the official evidence service.

---

## 7. Environment paths and MT5 identity

Recorded environment values (verify on the target machine; do not treat as universal constants):

    MT5_CLOCK_HISTORY_FILE=C:/Users/alial/AppData/Roaming/MetaQuotes/Terminal/Common/Files/GoldBot/clock_history.csv
    BROKER_OFFSET_REGISTRY_FILE=data/broker_offsets.jsonl
    BROKER_OFFSET_POLL_INTERVAL_SECONDS=1.0
    MT5_CLOCK_HISTORY_MAX_AGE_SECONDS=180

Checkpoint beside the clock-history file:

    clock_history.checkpoint.json

These are operational evidence files. Do not manually edit, truncate, regenerate, or overwrite the real files during diagnostics. Keep tests isolated from production evidence paths.

Previously observed MT5 identity (reconfirm before future broker-specific evidence collection):
- Account: 337260380
- Server: XMGlobal-MT5 9
- Symbol: GOLD
- Terminal build: 6316

These are observations from the working environment, not constants for all deployments.

---

## 8. Safety state

Last recorded environment:

    BOT_MODE=ANALYSIS_ONLY
    TRADING_ENABLED=False
    LIVE_TRADING_AUTHORIZED=False
    EMERGENCY_STOP=True
    MT5_ENABLED=True

MT5 connectivity and read-only collection may be enabled. Trading remains disabled and emergency stop asserted. This work does not authorize order placement, account changes, or trading activation.

Do not infer permission to start the backend/live service from this document. Start it only when explicitly requested and the operation is safe.

---

## 9. MT5 progress — keep separate from Chrony

### Test 1: connection and tick reception
Previously passed. The terminal connected and GOLD Bid/Ask/tick data was received. This proves connectivity at that test time; it does not prove historical candle backfill correctness.

### Test 2: quote persistence and integrity
Previously passed using:
- backend/mt5/quote_repository.py
- BrokerQuoteRepository
- migration 0008_broker_quote_persistence.py
- quote worker persistence integration
- scripts/diagnostics/verify_live_mt5_tick_persistence.py

Recorded live evidence included insert/readback, duplicate detection, and unchanged row count after a duplicate. Broker timestamp remained raw milliseconds; receipt time was stored separately in UTC.

### Historical candles and Backfill
**Not yet accepted/completed.** This is the next major data-evidence stage. Do not mark it complete based on Test 1, Test 2, Chrony checks, or unit tests alone.

---

## 10. Next work — safe order

1. Reconfirm Chrony status through the production ChronyStatusService path.
2. Verify NtpVerificationMonitor uses the same strict verifier and quorum policy; do not introduce a weaker alternate verifier.
3. Verify BrokerOffsetEvidenceService and BrokerOffsetEvidenceMonitor consume fresh trusted NTP evidence and the configured MT5 clock-history source.
4. Inspect clock-history age, row continuity, NTP verification flags, sample counts, account/server/symbol identity, and timestamp quality without modifying real evidence.
5. When evidence is sufficient, append Registry records only through the official evidence service.
6. Review offset transition coverage and interval gaps before authorizing historical candle conversion.
7. Design and run Historical Candles/Backfill in read-only or isolated mode first. Verify UTC conversion, restart/resume, idempotency, duplicates, and missing intervals.
8. Keep trading disabled throughout.

Historical Registry sample from Nov 18–20, 2026 contained 288 records and substantial coverage gaps; it is not sufficient by itself for safe historical backfill. A Nov 23 diagnostic CSV had 896 rows and NTP verification flags, but it was diagnostic-only, not official Registry evidence.

---

## 11. Failure diagnosis guide

When status fails, capture evidence from the same pinned Ubuntu socket and the production application path. Identify which layer failed:

- WSL distribution unavailable or cold-start timeout;
- Ubuntu Chrony service inactive;
- Unix socket missing or inaccessible;
- sudoers mismatch/password prompt;
- DNS/network/NTS source failure;
- fewer than three selectable sources;
- source-provider identity mismatch against authdata;
- tracking unsynchronized or invalid stratum/leap state;
- stale or insufficient broker clock-history evidence.

Do not conclude NTS is broken from one failed snapshot. Preserve raw tracking, sources, authdata, journal, and application error.

Never:
- switch production queries back to UDP localhost;
- open Chrony command access to LAN;
- weaken quorum;
- remove providers or PHC0 as a quick workaround;
- change Hyper-V time synchronization;
- manually modify Registry/checkpoint/clock-history evidence;
- start trading or place test orders.

---

## 12. Code and test entry points

| Purpose | Location |
|---|---|
| Chrony collection, parsing, quorum | backend/market_data/chrony.py |
| WSL keepalive lifecycle | backend/market_data/wsl_keepalive.py |
| Application lifespan integration | backend/main.py |
| NTP verification monitor | backend/market_data/ntp_monitor.py |
| Chrony tests | tests/test_chrony.py |
| Keepalive tests | tests/test_wsl_keepalive.py |
| Lifespan tests | tests/test_main_lifespan.py |
| Full test suite | tests/ |

Broker offset service/monitor implementation is under backend/market_data; locate the current implementation before changing it. Always inspect current main source and tests rather than assuming this document overrides code.

---

## 13. Handover checklist

- [ ] Read this document before touching time, NTS, WSL, UTC conversion, or broker-offset code.
- [ ] Confirm branch and working tree; do not overwrite uncommitted local work.
- [ ] Read chrony.py, wsl_keepalive.py, main.py, and related tests.
- [ ] Preserve socket pinning and the one-session WSL bundle.
- [ ] Preserve strict three-provider quorum and hostname identity matching.
- [ ] Confirm external sudoers and Ubuntu Chrony configuration on the target machine.
- [ ] Keep real clock-history and Registry evidence untouched during diagnostics.
- [ ] Keep trading disabled.
- [ ] Report CI evidence separately from live environment evidence.
- [ ] Update this document whenever an accepted decision, dependency, verified result, or milestone changes.

**Handover rule:** If a proposed change conflicts with an accepted decision above, stop and document the reason and impact before implementing it.
