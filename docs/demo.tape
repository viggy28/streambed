# Streambed Demo — split-pane via tmux
# Install: brew install vhs tmux
# Record:  vhs demo.tape
#
# Prerequisites:
#   pgbench data loaded and streambed sync fully caught up.
#   Postgres on port 5432, streambed query server on port 5433.

Output docs/demo.gif

Set Shell "bash"
Set FontSize 14
Set Width 1800
Set Height 700
Set Padding 10
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms

# --- Hidden setup: create tmux session with labeled panes ---
Hide
Type "export PATH=/opt/homebrew/bin:$PATH"
Enter
Sleep 500ms
Type "tmux kill-session -t demo 2>/dev/null; true"
Enter
Sleep 500ms

# Create session. Left pane = pane 0, will be Postgres.
Type "tmux new-session -d -s demo -x 200 -y 45"
Enter
Sleep 500ms

# Split right. Right pane = pane 1, will be Streambed.
Type "tmux split-window -h -t demo"
Enter
Sleep 500ms

# Set PGPASSWORD only in the Postgres pane (pane 0).
Type "tmux send-keys -t demo:0.0 'export PGPASSWORD=test' Enter"
Enter
Sleep 300ms

# Label each pane using a banner echo.
Type `tmux send-keys -t demo:0.0 'echo "═══ POSTGRES (port 5432) ═══"' Enter`
Enter
Sleep 300ms
Type `tmux send-keys -t demo:0.1 'echo "═══ STREAMBED (port 5433) ═══"' Enter`
Enter
Sleep 300ms

# Make sure pane 0 (left/Postgres) is active when we attach.
Type "tmux select-pane -t demo:0.0"
Enter
Sleep 300ms

# Attach to see the split panes.
Type "tmux attach -t demo"
Enter
Sleep 1s
Show

# --- Show row counts in each pane ---

# Left pane (Postgres) is active after attach.
Type `psql -h localhost -p 5432 -U postgres -c "SELECT count(*) AS accounts FROM pgbench_accounts"`
Enter
Sleep 3s

# Switch to right pane (Streambed).
Ctrl+b
Type "o"
Sleep 500ms

Type `psql -h localhost -p 5433 -U postgres -c "SELECT count(*) AS accounts FROM pgbench_accounts"`
Enter
Sleep 3s

# --- Fire the analytical query on both panes ---
# Type the query in the right pane (Streambed) first but DON'T press Enter yet.

Set TypingSpeed 1ms
Type `time psql -h localhost -p 5433 -U postgres -c "SELECT a.aid, a.bid, a.abalance, (SELECT count(*) FROM pgbench_history h WHERE h.aid = a.aid) AS my_txns, (SELECT avg(cnt) FROM (SELECT count(*) AS cnt FROM pgbench_history h2 JOIN pgbench_accounts a2 ON a2.aid = h2.aid WHERE a2.bid = a.bid GROUP BY h2.aid) sub) AS branch_avg_txns FROM pgbench_accounts a WHERE a.abalance > 0 ORDER BY my_txns DESC, a.aid ASC LIMIT 20"`
Set TypingSpeed 30ms
Sleep 500ms

# Switch to left pane (Postgres), type and run the query.
Ctrl+b
Type "o"
Sleep 500ms

Set TypingSpeed 1ms
Type `time psql -h localhost -p 5432 -U postgres -c "SELECT a.aid, a.bid, a.abalance, (SELECT count(*) FROM pgbench_history h WHERE h.aid = a.aid) AS my_txns, (SELECT avg(cnt) FROM (SELECT count(*) AS cnt FROM pgbench_history h2 JOIN pgbench_accounts a2 ON a2.aid = h2.aid WHERE a2.bid = a.bid GROUP BY h2.aid) sub) AS branch_avg_txns FROM pgbench_accounts a WHERE a.abalance > 0 ORDER BY my_txns DESC, a.aid ASC LIMIT 20"`
Set TypingSpeed 30ms
Enter

# Immediately switch to right pane and press Enter there too.
Sleep 200ms
Ctrl+b
Type "o"
Sleep 200ms
Enter

# Both queries are now running. Streambed finishes in ~2s.
# Stay on the right pane so the viewer sees Streambed results appear.
# Then Postgres keeps grinding on the left.
Sleep 15s
