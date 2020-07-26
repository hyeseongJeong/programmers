SELECT TO_CHAR(st.ts, 'yyyy-mm') AS dt, COUNT(DISTINCT userid) AS cnt
FROM raw_data.session_timestamp st
JOIN raw_data.user_session_channel usc ON st.sessionid = usc.sessionid
GROUP BY dt
ORDER BY dt ASC;