GOLD BOT — MASTER SPECIFICATION

Project: GOLD BOT
Repository: MTb
Architecture Version: 1.0
Status: Initial Architecture
Platform: MetaTrader 5
Backend: Python
Server OS: Windows
Client: Android
Cost Policy: Free Components Only

---

1. PROJECT PURPOSE

GOLD BOT is a complete automated trading system for MetaTrader 5.

Its purpose is to:

- Collect real market data.
- Validate market data and timestamps.
- Analyze the market using ten core analysis engines.
- Combine analysis evidence into a trading decision.
- Calculate trading risk.
- Monitor account balance, equity, margin, and positions.
- Execute and manage trades when explicitly authorized.
- Provide complete remote control through an Android application.
- Record all important operations and decisions.

This is a production-oriented project. It must not contain fake implementations, empty modules, demonstration-only components, or fabricated results.

2. NON-NEGOTIABLE REQUIREMENTS

1. The entire project must use free components and services.
2. Paid APIs, subscriptions, hosting, and software licenses are excluded.
3. No fabricated market data or analysis evidence.
4. No arbitrary confidence percentages.
5. No duplicate module responsibilities.
6. No unnecessary files or dependencies.
7. No empty files or placeholder implementations.
8. Every component must have a real purpose.
9. Every component must have a defined interface.
10. Every critical component must have automated tests.
11. Integration must be verified.
12. Errors must be handled explicitly.
13. Unavailable data must be identified as unavailable.
14. Live trading must never activate automatically.
15. Security must be considered from the beginning.

3. SUPPORTED PLATFORM

The primary trading platform is MetaTrader 5.

The system should support compatible MT5 demo and real trading accounts, subject to broker capabilities and account permissions.

The system must not assume that all brokers provide identical symbols, execution rules, volume limits, or market data.

Broker specifications must be retrieved and validated before trading operations.

4. SYSTEM ARCHITECTURE

4.1 Android Application

The Android application is the user's control and monitoring interface.

It must support:

- Secure authentication.
- Backend connection status.
- MT5 connection status.
- Account information.
- Balance and equity.
- Margin and free margin.
- Open positions.
- Trading symbol selection.
- Timeframe selection.
- Lot configuration.
- Risk configuration.
- Analysis results.
- Trading mode selection.
- Automated trading authorization.
- Emergency stop.
- Notifications.
- Activity and error history.

The Android application must not independently execute trading decisions.

4.2 Windows Backend

The Windows computer is the central system.

It must provide:

- API server.
- Authentication and authorization.
- MT5 connection management.
- Market data ingestion.
- Market data validation.
- Analysis orchestration.
- Decision fusion.
- Risk management.
- Trade execution management.
- Position monitoring.
- Database and persistence.
- Logging and auditing.
- Android communication.

All important trading decisions must be controlled by the backend.

4.3 MetaTrader 5 Integration

The MT5 integration must provide verified access to:

- Account information.
- Symbol specifications.
- Market prices.
- Historical candles.
- Live market updates.
- Positions.
- Orders.
- Trading permissions.
- Order execution when authorized.
- Position modification and closing when authorized.
- Execution results.

Every execution result must be verified against the actual MT5 response and resulting account state.

5. CORE ANALYSIS ENGINES

The project must contain exactly ten primary analysis engines.

1. Trend Analysis.
2. Multi-Timeframe Analysis.
3. Market Structure Analysis.
4. Price Action Analysis.
5. Technical Indicators Analysis.
6. Liquidity Analysis.
7. Supply and Demand Analysis.
8. Volume Analysis.
9. Market Regime and Sessions Analysis.
10. Quantitative Analysis.

Each engine must:

- Have a defined input contract.
- Validate its input.
- Preserve source timestamps.
- Report insufficient data.
- Report unavailable data.
- Return structured results.
- Explain its findings.
- Avoid inventing evidence.
- Be independently testable.
- Be integrated into the central analysis service.

The ten engines must not duplicate each other's responsibilities.

6. SUPPORTING SYSTEMS

The following supporting systems are required:

- Market Data Validator.
- Decision Fusion Engine.
- Risk Management Engine.
- Account and Portfolio Manager.
- Signal Lifecycle Manager.
- Execution Manager.
- MT5 Connection Manager.
- Historical Backtesting Engine.
- Audit and Monitoring System.
- Configuration Manager.
- Authentication and Authorization.
- Notification Service.

Supporting systems must remain separate from the ten primary analysis engines.

7. MARKET DATA REQUIREMENTS

The system must distinguish between:

- OHLC prices.
- Tick volume.
- Real volume.
- Bid price.
- Ask price.
- Spread.
- Candle opening time.
- Candle closing status.
- Data source.
- Data freshness.

Tick volume must never be represented as real traded volume.

Candle-based estimates must never be represented as true order flow.

Liquidity zones inferred from price must never be described as confirmed pending orders or actual stop-loss locations.

Unavailable order-book, options, news, economic calendar, or external market data must not be fabricated.

All internal timestamps must be timezone-aware and normalized.

Invalid, stale, duplicated, incomplete, or future-dated data must not silently enter the trading decision process.

8. ANALYSIS AND DECISION FUSION

The central analysis service must coordinate all ten engines.

The Decision Fusion Engine must:

- Receive structured analysis evidence.
- Validate evidence source.
- Validate evidence timestamps.
- Identify unavailable and insufficient evidence.
- Distinguish directional evidence from descriptive evidence.
- Detect conflicting evidence.
- Avoid double-counting correlated evidence.
- Evaluate evidence quality.
- Produce BUY, SELL, WAIT, or NO_TRADE.
- Explain supporting and opposing evidence.
- Preserve the original engine results.

Confidence must not be fabricated by converting unrelated scores into arbitrary percentages.

A trading decision is not automatically permission to execute a trade.

9. RISK MANAGEMENT

Risk management must independently validate:

- Account balance.
- Account equity.
- Available margin.
- Maximum risk per trade.
- Daily loss limit.
- Maximum open positions.
- Maximum total exposure.
- Lot size.
- Stop loss.
- Take profit.
- Spread.
- Broker volume limits.
- Minimum stop distance.
- Trading permissions.
- Existing positions and pending orders.

Invalid or missing risk parameters must block execution.

The system must not increase risk automatically without explicit user configuration.

Lot calculation must use the actual symbol specifications and account currency. A fixed lot value alone is not sufficient to establish safe risk.

10. TRADING MODES

The system must support:

ANALYSIS_ONLY

- Market analysis is allowed.
- Decisions may be generated.
- No orders may be sent.

DEMO_AUTOMATED

- Requires a verified demo account.
- Trading operations are restricted to the demo account.
- All risk controls remain active.

LIVE_AUTOMATED

- Requires explicit user authorization.
- Requires verified account identity and trading permissions.
- Requires successful safety checks.
- Must never activate by default.

Changing trading mode must require authentication and must be recorded in the audit log.

The emergency stop must block new entries immediately. Position closing or other protective actions must follow an explicitly defined and tested safety policy.

11. USER CONFIGURATION

The user must be able to configure:

- Trading symbol.
- Primary timeframe.
- Additional analysis timeframes.
- Fixed lot or risk-based sizing.
- Maximum risk per trade.
- Daily loss limit.
- Maximum open positions.
- Stop-loss policy.
- Take-profit policy.
- Trading mode.
- Automated trading authorization.

All settings must be validated against the connected account and broker specifications.

The application must clearly display invalid, unavailable, pending, and accepted settings.

12. FREE-ONLY POLICY

Only free components with suitable licenses may be used.

The following are excluded when they require payment:

- Market data APIs.
- Cloud hosting.
- Paid databases.
- Trading signal services.
- External analysis services.
- Software licenses.
- Subscription-based services.

The Windows home computer is the intended backend host.

If a feature has no reliable free implementation, it must be marked UNAVAILABLE or excluded.

No paid service may be introduced without changing this specification and receiving explicit user approval.

13. SECURITY REQUIREMENTS

Required controls:

- Authenticated API access.
- Authorization checks.
- Secure secret storage.
- Encrypted network communication.
- Input validation.
- Replay protection for sensitive commands.
- Rate limiting.
- Audit logs.
- Secure Android-to-backend communication.
- Protection of MT5 credentials.
- No passwords or secrets in application logs.

Trading commands must never be available to unauthenticated clients.

14. DATA STORAGE

The project must persist the information necessary for:

- Market data history.
- Analysis results.
- Trading decisions.
- Account snapshots.
- Position history.
- Execution results.
- Risk events.
- Configuration changes.
- Security events.
- System errors.

Data storage must have defined schemas, migrations, retention rules, and recovery procedures.

15. TESTING AND ACCEPTANCE

A component is complete only when:

1. Its implementation exists.
2. Its input and output contracts are defined.
3. Its dependencies are connected.
4. Its errors are handled.
5. Its unit tests pass.
6. Its integration tests pass.
7. Its behavior is documented.
8. Its actual runtime behavior has been verified.

Required test categories:

- Unit tests.
- Integration tests.
- Market data validation tests.
- Timestamp and freshness tests.
- Analysis engine tests.
- Decision fusion tests.
- Risk calculation tests.
- Security tests.
- MT5 connection tests.
- Demo execution tests.
- Position management tests.
- Backend recovery tests.
- Android communication tests.

A green build alone does not prove that the trading system works correctly.

16. DEVELOPMENT CONTROL

Development must proceed in complete, verified batches.

Every file must have a concrete responsibility.

No unfinished component may be described as complete.

No module may depend on an undocumented interface.

Architectural changes must update this specification and the affected interface documentation.

The repository must remain clean, maintainable, integrated, and free of unnecessary files.

17. FINAL ACCEPTANCE OBJECTIVE

The final system must allow the user to:

1. Connect a compatible MT5 account.
2. Select a trading symbol.
3. Select analysis timeframes.
4. Configure lot sizing and risk.
5. Receive complete market analysis.
6. Review the resulting decision and its evidence.
7. Monitor the account and open positions.
8. Authorize demo automated trading.
9. Monitor and manage demo trades.
10. Explicitly authorize live trading only after all required safety checks and acceptance tests pass.

The system must never claim that a feature works unless its implementation and integration have been verified.

---

END OF MASTER SPECIFICATION
