-- File: compare_hosts.sql
-- Question: Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)

SELECT 
    host, 
    AVG(event_count) AS avg_events_per_session,
    COUNT(*) AS total_sessions,
    MAX(session_end - session_start) AS longest_session_duration
FROM 
    session_events_sink
WHERE 
    host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY 
    host
ORDER BY 
    avg_events_per_session DESC;
