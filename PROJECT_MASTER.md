GOLD BOT — MASTER PROJECT SPECIFICATION

Project: GOLD BOT
Platform: MetaTrader 5 + Python Backend + Android Control App
Server: User-owned Windows computer
Primary Market: Gold (XAUUSD and broker-specific gold symbols)
Cost Policy: Free tools, free libraries, free data sources only
Development Standard: Production-oriented, fully integrated, tested implementation

---

1. PROJECT PURPOSE

Build a complete automated trading system for MetaTrader 5.

The system must be capable of:

- Connecting to supported MT5 trading accounts.
- Receiving and validating market data.
- Analyzing the market using ten core analytical domains.
- Evaluating multiple established trading strategies.
- Resolving strategy conflicts before producing a trading decision.
- Calculating position size and risk.
- Managing open positions and protective orders.
- Sending trading commands through a controlled MT5 integration.
- Providing complete monitoring and control through an Android application.

The architecture must support both demo and real MT5 accounts.

Real-account execution must remain explicitly disabled until all required validation, security, risk, and demo acceptance tests have passed and the owner deliberately enables it.

2. NON-NEGOTIABLE PROJECT RULES

1. No paid APIs, subscriptions, servers, data feeds, libraries, or licenses.
2. No fake, placeholder, empty, or decorative modules.
3. No duplicate engines with overlapping responsibilities.
4. No undocumented assumptions about market data.
5. No fabricated confidence scores or invented market information.
6. No strategy may bypass the central risk manager.
7. No strategy may send orders directly to MT5.
8. No trading command may be accepted without validation and authorization.
9. Every essential module must have a real implementation and meaningful tests.
10. Every module must have a defined owner, input contract, output contract, and integration path.
11. The Android application is the control and monitoring interface; the backend owns trading decisions.
12. The Windows computer is the user's server. The design must not require paid cloud hosting.
13. If a required external capability has no reliable free alternative, mark it "UNAVAILABLE" and prevent dependent decisions from using it.
14. Never describe a feature as complete merely because its file or interface exists.

3. SYSTEM ARCHITECTURE

The system consists of four main layers.

A. MT5 Integration Layer

Responsibilities:

- Connect to the installed MetaTrader 5 terminal.
- Identify the connected account and broker.
- Read account information and open positions.
- Retrieve candles and available market information.
- Preserve the original symbol, timeframe, timestamps, and volume source.
- Receive authorized commands from the backend.
- Execute commands only when the execution policy permits them.
- Return actual execution results and broker errors.
- Monitor connection loss and terminal availability.

The integration must account for broker-specific symbol names, digits, point size, contract size, volume step, minimum volume, maximum volume, trading sessions, and execution restrictions.

B. Python Backend

Responsibilities:

- Market data validation and storage.
- Data quality and freshness monitoring.
- Ten analytical domains.
- Strategy evaluation and conflict resolution.
- Decision generation.
- Risk and position sizing.
- Portfolio exposure management.
- Trade lifecycle management.
- MT5 command authorization.
- Audit logs and monitoring.
- API services for the Android application.

C. Android Control Application

Responsibilities:

- Secure user authentication.
- Backend connection status.
- MT5 connection and account status.
- Account balance, equity, margin, and open positions.
- Market analysis and evidence.
- Active strategy and decision explanations.
- Trading configuration.
- Risk settings.
- Enable/disable controls subject to backend authorization.
- Emergency stop.
- Logs, alerts, and system health.

The application must not independently calculate or execute trades.

D. Local Windows Server

Responsibilities:

- Run the backend.
- Run the required database.
- Communicate with MT5.
- Maintain logs and recovery information.
- Start and stop services safely.
- Recover from unexpected process failures.
- Restrict remote access to authenticated and authorized clients.

No assumption may be made that the computer is always online. Connection loss must lead to a safe operating state.

4. TEN CORE ANALYTICAL DOMAINS

The system must use exactly these ten core analytical domains.

1. Trend Analysis
   
   - Trend direction, strength, persistence, and reversals.

2. Multi-Timeframe Analysis
   
   - Agreement and disagreement between configured timeframes.

3. Market Structure
   
   - Swing highs/lows, BOS, CHoCH, structural confirmation.

4. Price Action
   
   - Candlestick patterns, rejection, momentum, and price behavior.

5. Technical Indicators
   
   - RSI, MACD, ATR, moving averages, Bollinger Bands, and other justified indicators.

6. Liquidity Analysis
   
   - Liquidity pools, equal highs/lows, sweeps, and stop-run-like price behavior.
   - These are price-based estimates, not verified broker stop orders.

7. Supply and Demand
   
   - Supply zones, demand zones, support, resistance, and zone validation.

8. Volume Analysis
   
   - Tick volume, real volume when genuinely supplied, relative volume, and volume profile when the underlying data supports it.
   - Never label tick volume as real traded volume.

9. Volatility and Market Regime
   
   - Volatility state, trending/ranging conditions, changing market behavior, and session context.

10. Quantitative Analysis
    
    - Statistical returns, distributions, historical behavior, and measurable strategy performance.

Session analysis may remain a distinct supporting implementation inside the ninth domain. It must not become an additional independent voting engine.

5. IMPORTANT SUPPORTING ENGINES

These are supporting system capabilities, not additional analytical domains:

- Market Data Validation.
- Data Quality and Freshness.
- Decision Fusion.
- Strategy Conflict Resolver.
- Risk Management.
- Position Sizing.
- Portfolio Exposure Management.
- Trade Lifecycle Management.
- Execution Quality Monitoring.
- Backtesting.
- Walk-Forward Validation.
- Strategy Performance Evaluation.
- Anomaly Detection.
- Audit Logging.
- Notifications.
- System Health Monitoring.
- Recovery and Reconnection.

Every supporting engine must have a real responsibility and must be connected to the relevant workflow.

6. TRADING STRATEGY LIBRARY

Strategies must be separate from the ten analytical domains.

The initial strategy library must cover these established approaches where their rules can be objectively defined and tested:

1. Trend Following.
2. Moving Average Crossover.
3. Trend Pullback.
4. Support and Resistance Reversal.
5. Breakout Trading.
6. Breakout Retest.
7. Range Trading.
8. Mean Reversion.
9. Price Action and Candlestick Reversal.
10. Supply and Demand Zone Trading.
11. Liquidity Sweep Reversal.
12. Market Structure BOS/CHoCH.
13. London Session and Opening Range Breakout.
14. New York Session and Opening Range Breakout.
15. Volatility Expansion and Contraction.

Each strategy must define:

- Required market data.
- Required analytical evidence.
- Entry conditions.
- Exit conditions.
- Stop-loss logic.
- Take-profit logic.
- Invalidating conditions.
- Applicable market regimes.
- Supported timeframes.
- Risk constraints.
- Strategy-specific tests.
- Historical performance measurements.

A strategy must not be enabled merely because its name or implementation exists.

Strategies must be validated independently before being eligible for the decision system.

No strategy is guaranteed to be profitable.

7. STRATEGY CONFLICT RESOLUTION

The system must not combine contradictory strategy signals blindly.

The Strategy Conflict Resolver must:

1. Evaluate the current market regime.
2. Determine which strategies are applicable.
3. Reject strategies with insufficient or invalid data.
4. Detect conflicting entry directions.
5. Detect correlated or duplicate strategy evidence.
6. Apply configured strategy priority and validation status.
7. Reject excessive disagreement.
8. Return "WAIT" or "NO_TRADE" when no reliable decision exists.
9. Record the reason for every accepted or rejected strategy.

A large number of agreeing strategies must not automatically create a high confidence score if they depend on the same underlying evidence.

8. MARKET DATA CONTRACT

Every candle must preserve:

- Broker symbol.
- Internal normalized symbol.
- Timeframe.
- Open time in UTC.
- Open.
- High.
- Low.
- Close.
- Tick volume.
- Real volume, if supplied.
- Spread, if supplied.
- Candle closure status.
- Original source.
- Receipt timestamp.
- Data validation status.

Requirements:

- Reject invalid OHLC relationships.
- Detect duplicate candles.
- Detect missing candles and time gaps.
- Reject future timestamps.
- Validate timezone handling.
- Distinguish closed candles from forming candles.
- Detect stale data.
- Preserve the actual volume source.
- Never invent missing values.
- Never use unverified data for a trading decision.

The system must define how many candles each analytical engine requires. Insufficient data must produce an explicit insufficient-data state.

9. DECISION CONTRACT

Every final decision must contain:

- Symbol.
- Timeframe.
- Reference timestamp.
- Decision: "BUY", "SELL", "WAIT", or "NO_TRADE".
- Supporting evidence.
- Opposing evidence.
- Applicable strategies.
- Rejected strategies and reasons.
- Market regime.
- Data quality.
- Data freshness.
- Risk assessment.
- Proposed entry conditions.
- Proposed stop loss.
- Proposed take profit.
- Position sizing result.
- Decision validity period.
- Execution authorization state.

Confidence must not be fabricated from unrelated scores.

If a confidence measure is used, its meaning and calibration method must be documented and validated.

10. RISK MANAGEMENT

The risk manager is mandatory and independent of strategy decisions.

It must validate:

- User-defined risk per trade.
- Maximum daily loss.
- Maximum total exposure.
- Maximum open positions.
- Maximum spread.
- Maximum slippage where measurable.
- Minimum margin requirements.
- Broker volume limits.
- Stop-loss validity.
- Available account equity.
- Trading session restrictions.
- Duplicate command prevention.
- Existing positions and pending orders.

Position size must be calculated using the actual broker contract specifications and account currency.

If required contract or conversion information is unavailable, the system must refuse to calculate an executable volume.

A strategy must never override risk rejection.

11. EXECUTION SECURITY

All order execution must pass through one controlled execution gateway.

The gateway must validate:

- Account identity.
- Account mode.
- User authorization.
- Current system mode.
- Command expiration.
- Unique command identifier.
- Symbol availability.
- Volume validity.
- Stop-loss and take-profit validity.
- Current risk authorization.
- Current market data freshness.
- Duplicate execution protection.

The gateway must record the request, broker response, actual fill, and any failure.

The system must distinguish between a command being accepted, submitted, filled, partially filled, rejected, or cancelled.

A network timeout must never be interpreted as proof that an order was not executed.

12. OPERATING MODES

The system must implement explicit modes:

- "OFFLINE"
- "ANALYSIS_ONLY"
- "DEMO_TRADING"
- "LIVE_TRADING_LOCKED"
- "LIVE_TRADING_AUTHORIZED"
- "EMERGENCY_STOP"

The initial installation must not authorize live execution.

Live execution requires deliberate user authorization and successful validation of all safety prerequisites.

Emergency stop must prevent new entries and follow a documented policy for existing positions. It must not silently close positions without an explicit, tested policy.

13. ANDROID CONFIGURATION

The user must be able to configure:

- Trading symbol.
- Main timeframe.
- Additional analysis timeframes.
- Lot mode: fixed or risk-based.
- Fixed lot size, when selected.
- Risk percentage.
- Stop-loss policy.
- Take-profit policy.
- Maximum open positions.
- Maximum daily loss.
- Allowed strategies.
- Strategy priorities.
- Operating mode.

All settings must be validated by the backend.

The application must show the effective settings and the reason whenever a setting is rejected.

14. SECURITY AND CONNECTIVITY

Requirements:

- Authenticated API access.
- Encrypted network communication.
- Secure secret storage.
- No hardcoded passwords or API secrets.
- Request authentication and authorization.
- Rate limiting.
- Replay protection.
- Command expiration.
- Audit trail.
- Secure Android-to-backend communication.
- Restricted Windows firewall access.
- Safe handling of backend restarts.
- No public unauthenticated MT5 command endpoint.

The backend must not expose the MT5 terminal directly to the public internet.

Remote access must use a secure, free solution or remain unavailable until a safe solution is configured.

15. FREE-ONLY TECHNOLOGY POLICY

Every dependency and external service must be reviewed for:

- License.
- Commercial-use restrictions.
- Subscription requirements.
- API limits.
- Data access restrictions.
- Long-term availability.

Paid-only capabilities must not be silently included.

Free services must not be described as unlimited or guaranteed.

If a capability has no reliable free solution, document it as "UNAVAILABLE" and disable dependent functionality.

16. TESTING AND ACCEPTANCE

Testing must cover:

1. Unit tests for every core engine.
2. Data contract tests.
3. Timestamp and timezone tests.
4. Missing and stale data tests.
5. Strategy rule tests.
6. Strategy conflict tests.
7. Risk calculation tests.
8. Broker specification tests.
9. Duplicate command tests.
10. Connection loss tests.
11. Backend restart and recovery tests.
12. API authentication tests.
13. Android integration tests.
14. MT5 demo integration tests.
15. End-to-end demo trading tests.
16. Emergency stop tests.
17. Historical and walk-forward strategy validation.

A successful build alone does not prove that trading logic is correct.

No live execution authorization may be granted until the required acceptance criteria are documented and passed.

17. DEVELOPMENT ORDER

The project must be implemented in dependency order:

1. Repository architecture and project configuration.
2. Shared schemas and contracts.
3. Configuration and security.
4. Database and persistence.
5. MT5 connectivity and market data ingestion.
6. Data validation and quality.
7. Ten analytical domains.
8. Strategy library.
9. Strategy conflict resolution.
10. Decision fusion.
11. Risk and position sizing.
12. Controlled execution gateway.
13. Trade lifecycle and account monitoring.
14. Backend API.
15. Windows service operation and recovery.
16. Android control application.
17. Full integration and acceptance testing.

Do not build later layers on top of incomplete contracts.

18. FILE AND MODULE QUALITY STANDARD

Every committed file must:

- Have a specific responsibility.
- Contain a real implementation appropriate to that responsibility.
- Use documented interfaces.
- Avoid duplicated logic.
- Handle errors explicitly.
- Have meaningful tests where applicable.
- Be integrated into the actual application path.
- Be included in build and test workflows when relevant.

Do not create files just to make the architecture look complete.

Do not leave required behavior as TODO, placeholder, mock-only, or simulated behavior in the production path.

Test doubles may exist only inside clearly identified tests.

19. PROJECT COMPLETION DEFINITION

The project is not complete until:

- MT5 connectivity is proven end to end.
- Market data is validated and traceable.
- All ten analytical domains work with real supported inputs.
- Strategy rules are implemented and tested.
- Strategy conflicts are handled safely.
- Decision outputs are explainable.
- Risk calculations use actual broker specifications.
- Execution is controlled by one authorization gateway.
- Demo trading works end to end.
- Android controls work against the real backend.
- Recovery, security, and emergency stop are tested.
- No essential feature depends on a paid service.
- All known unavailable capabilities are explicitly documented.

This document is the governing specification. Any change to its architecture or safety rules must be deliberate, documented, and justified.
