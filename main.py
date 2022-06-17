import argparse

from pyhive import hive


def init_argparse():
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument("-i", "--hiveHost", help="hive host ip address", required=True)
    parser.add_argument("-p", "--port", help="hive port", required=True)
    parser.add_argument("-u", "--username", help="hive username", required=True)
    parser.add_argument("-w", "--passwd", help="hive password", required=False)
    parser.add_argument("-d", "--database", help="database", required=True)
    parser.add_argument("-f", "--hdfsAddress", help="hadoop hdfs address", required=True)

    # Read arguments from command line
    args = parser.parse_args()

    return args


def connect_to_hive(host, port, username, database, password):
    conn = hive.Connection(host=host, port=port, username=username, database=database, password=password)
    return conn


def get_hive_table(conn):
    cursor = conn.cursor()
    cursor.execute("show tables")

    table_name = []

    for table in cursor.fetchall():
        table_name.append("".join(table))

    return table_name

def table_name_to_txt():



if __name__ == '__main__':
    args = init_argparse()
    conn = connect_to_hive(args.hiveHost, args.port, args.username, args.database, args.passwd)

    print(get_hive_table(conn))