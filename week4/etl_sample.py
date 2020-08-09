import psycopg2
import requests


def get_redshift_connection():
    host = "grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "tjdehfldk"
    redshift_pass = "Tjdehfldk1!"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()


def load(text):
    cur = get_redshift_connection()
    for r in text[1:]:
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql = "INSERT INTO tjdehfldk.name_gender VALUES ('{name}', '{gender}')".format(name=name, gender=gender)
            print(sql)
            cur.execute(sql)


if __name__ == "__main__":
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    data = requests.get(link)
    lines = data.text.split("\n")
    load(lines)
