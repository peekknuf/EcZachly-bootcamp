-- File: average_events_tech_creator.sql
-- Question: What is the average number of web events of a session from a user on Tech Creator?

SELECT 
    host, 
    AVG(event_count) AS avg_events_per_session
FROM 
    session_events_sink
WHERE 
    host LIKE '%techcreator.io%'
GROUP BY 
    host;
