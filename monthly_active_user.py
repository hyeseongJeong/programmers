from redshift_connection import get_redshift_connection


if __name__ == "__main__":
    cur = get_redshift_connection()

    sql = "SELECT TO_CHAR(st.ts, 'yyyy-mm') AS dt, COUNT(DISTINCT userid) AS cnt \
           FROM raw_data.session_timestamp st \
           JOIN raw_data.user_session_channel usc ON st.sessionid = usc.sessionid \
           GROUP BY dt \
           ORDER BY dt ASC;"
    cur.execute(sql)
    rows = cur.fetchall()

    for r in rows:
        print(r)
