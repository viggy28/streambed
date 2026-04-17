# Streambed Demo — split-pane via tmux
# Install: brew install vhs tmux
# Record:  vhs demo.tape
#
# Prerequisites:
#   pgbench data loaded and streambed sync fully caught up.
#   Postgres on port 5432, streambed query server on port 5433.

Output docs/demo.gif

Set Shell "bash"
Set FontSize 16
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

# Set PGPASSWORD in both panes.
Type "tmux send-keys -t demo:0.0 'export PGPASSWORD=test' Enter"
Enter
Sleep 300ms
Type "tmux send-keys -t demo:0.1 'export PGPASSWORD=test' Enter"
Enter
Sleep 300ms

# Enable persistent pane title bars at top.
Type `tmux set -t demo pane-border-status top`
Enter
Sleep 200ms
Type `tmux set -t demo pane-border-format '#{?pane_active,#[fg=green bold],#[fg=white]} #{pane_title} '`
Enter
Sleep 200ms

# Set pane titles (persistent — won't scroll away).
Type `tmux select-pane -t demo:0.0 -T 'POSTGRES (port 5432)'`
Enter
Sleep 200ms
Type `tmux select-pane -t demo:0.1 -T 'STREAMBED (port 5433)'`
Enter
Sleep 200ms

# Colored banners as initial visual.
Type `tmux send-keys -t demo:0.0 'echo -e "\e[1;33m═══ POSTGRES (port 5432) ═══\e[0m"' Enter`
Enter
Sleep 300ms
Type `tmux send-keys -t demo:0.1 'echo -e "\e[1;36m═══ STREAMBED (port 5433) ═══\e[0m"' Enter`
Enter
Sleep 300ms

# Pre-type the count query in both panes (no Enter yet).
Type `tmux send-keys -t demo:0.0 'psql -h localhost -p 5432 -U postgres -c "SELECT count(*) AS accounts FROM pgbench_accounts"'`
Enter
Sleep 300ms
Type `tmux send-keys -t demo:0.1 'psql -h localhost -p 5433 -U postgres -c "SELECT count(*) AS accounts FROM pgbench_accounts"'`
Enter
Sleep 300ms

# Make sure pane 0 (left/Postgres) is active when we attach.
Type "tmux select-pane -t demo:0.0"
Enter
Sleep 300ms

# Attach to see the split panes (both have count query pre-typed).
Type "tmux attach -t demo"
Enter
Sleep 1s
Show

# --- Fire count queries: press Enter in each pane ---
Sleep 1s
Enter
Sleep 100ms
Ctrl+b
Type "o"
Sleep 200ms
Enter
Sleep 3s

# --- Pre-stage the analytical query (detach, send-keys, re-attach) ---
Hide
Ctrl+b
Type "d"
Sleep 500ms
Type `tmux send-keys -t demo:0.0 'time psql -h localhost -p 5432 -U postgres -c "SELECT a.aid, a.bid, a.abalance, (SELECT count(*) FROM pgbench_history h WHERE h.aid = a.aid) AS my_txns, (SELECT avg(cnt) FROM (SELECT count(*) AS cnt FROM pgbench_history h2 JOIN pgbench_accounts a2 ON a2.aid = h2.aid WHERE a2.bid = a.bid GROUP BY h2.aid) sub) AS branch_avg_txns FROM pgbench_accounts a WHERE a.abalance > 0 ORDER BY my_txns DESC, a.aid ASC LIMIT 20" && echo -e "\e[1;32m✓ Done\e[0m"'`
Enter
Sleep 300ms
Type `tmux send-keys -t demo:0.1 'time psql -h localhost -p 5433 -U postgres -c "SELECT a.aid, a.bid, a.abalance, (SELECT count(*) FROM pgbench_history h WHERE h.aid = a.aid) AS my_txns, (SELECT avg(cnt) FROM (SELECT count(*) AS cnt FROM pgbench_history h2 JOIN pgbench_accounts a2 ON a2.aid = h2.aid WHERE a2.bid = a.bid GROUP BY h2.aid) sub) AS branch_avg_txns FROM pgbench_accounts a WHERE a.abalance > 0 ORDER BY my_txns DESC, a.aid ASC LIMIT 20" && echo -e "\e[1;32m✓ Done\e[0m"'`
Enter
Sleep 300ms
Type "tmux select-pane -t demo:0.0"
Enter
Sleep 200ms
Type "tmux attach -t demo"
Enter
Sleep 500ms
Show

# --- Fire the analytical query: press Enter in each pane ---
Sleep 1s
Enter
Sleep 100ms
Ctrl+b
Type "o"
Sleep 200ms
Enter

# Both queries are now running. Streambed finishes in ~2s.
# Postgres keeps grinding on the left — viewer sees the difference.
Sleep 15s
