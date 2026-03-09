## Summary
- deepen internal adaptive execution learning
- add shared training/live learning feedback layer
- add long-window execution stress mode
- add symbol execution quality memory and adaptive symbol cooldown
- tighten anti-overtrade logic and pair ranking using execution quality

## What Changed
1. Shared learning layer
- added shared adaptive feedback that learns from both training and live cycles
- blends cycle quality, trade quality, pass rate, confidence, and edge margin into a shared bias
- uses training as seed data and automatically gives more weight to live once enough live data exists

2. Execution stress mode
- added long-window execution stress detection based on stale data rate, blocked-rate, decision quality, API/logic errors, slow cycles, and slippage
- automatically tightens edge/quality/confidence thresholds and reduces risk when execution degrades

3. Symbol execution memory
- added per-symbol execution slippage EMA
- added per-symbol execution quality EMA
- execution quality now affects risk sizing, TP/SL/TS adaptation, and pair selection

4. Adaptive symbol cooldown and anti-overtrade
- added self-healing symbol cooldown derived from recent cycle behavior, stale windows, loss streaks, and weak execution
- prevents repeated entries into toxic market windows without manual tuning

5. Runtime diagnostics
- added runtime payload fields for shared learning bias, execution stress, adaptive symbol cooldown, and symbol execution metrics

## Expected Impact
- less overtrading in weak or stale market windows
- better pair selection under execution degradation
- tighter autonomous adaptation without manual retuning
- stronger bridge between training behavior and live filtering

## Validation
- `python -m py_compile mexc_bot.py`
- `python -m py_compile mexc_bot.py mexc_bot_gui_web.py`
- synced updated bot core into the GitHub-clean copy and pushed branch `fix/gui-freeze-overnight`

## Notes
- changes are internal only
- no GUI logic changes in this PR
- main branch has moved ahead with repository/community files, so this update is pushed as a branch for review
