import argparse

from pyhive import hive


def init_argparse():
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument("-i", "--hive-host-address", help="hive address", required=True)
    parser.add_argument("-p", "--port", help="hive port", required=True)
    parser.add_argument("-u", "--username", help="hive username", required=True)
    parser.add_argument("-d", "--database", help="database", required=True)
    parser.add_argument("-f", "--hdfs-address", help="hadoop hdfs address", required=True)

    # Read arguments from command line
    args = parser.parse_args()

    return args


def connect_to_hive():
    conn = hive.Connection(host="39.103.230.188", port=30115, username="dehoop", database="rd_dev")


def get_hive_table(conn):
    cursor = conn.cursor()
    cursor.execute("show tables")

    table_name = []

    for table in cursor.fetchall():
        table_name.append("".join(table))

    return table_name


if __name__ == '__main__':
    init_argparse()
    # connect_to_hive()
