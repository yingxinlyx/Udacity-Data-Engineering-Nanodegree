import configparser
import psycopg2
from sql_queries import analytic_queries


def analytic(cur, conn):
    """execute analytical queries and print results"""
    
    for query in analytic_queries:
        cur.execute(query)
        res = cur.fetchone()

        for row in res:
            print("   ", row)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    analytic(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
