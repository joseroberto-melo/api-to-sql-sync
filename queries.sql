-- Example queries to run against data/sync.db after a sync
-- (use any SQLite client, e.g. DB Browser for SQLite, or `sqlite3 data/sync.db`).

-- 1. Authors with the most posts
SELECT u.name, u.company, COUNT(p.id) AS posts
FROM users u
LEFT JOIN posts p ON p.user_id = u.id
GROUP BY u.id
ORDER BY posts DESC, u.name;

-- 2. Users by city
SELECT city, COUNT(*) AS users FROM users GROUP BY city ORDER BY users DESC;

-- 3. Audit trail: what did each sync run do?
SELECT run_id, started_at, finished_at, status, details
FROM sync_runs
ORDER BY run_id DESC;

-- 4. Rows changed in the most recent run (inserted or updated)
SELECT 'users' AS tbl, id, synced_at FROM users
WHERE synced_at = (SELECT MAX(synced_at) FROM users)
UNION ALL
SELECT 'posts', id, synced_at FROM posts
WHERE synced_at = (SELECT MAX(synced_at) FROM posts);
