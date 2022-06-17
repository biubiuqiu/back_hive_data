import argparse
import logging
import warnings

from pyhive import hive
from datetime import datetime

# 初始化日志以及警告配置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s : %(levelname)s  %(message)s',  # 定义输出log的格式
                    datefmt='%Y-%m-%d %A %H:%M:%S')
warnings.filterwarnings('ignore')


def init_argparse():
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument("-i", "--hiveHost", help="hive host ip address", required=True)
    parser.add_argument("-p", "--port", help="hive port", required=True)
    parser.add_argument("-u", "--username", help="hive username", required=True)
    parser.add_argument("-w", "--passwd", help="hive password", required=False)
    parser.add_argument("-d", "--database", help="database", required=True)
    parser.add_argument("-f", "--hdfsAddress", help="hadoop hdfs address", required=True)
    parser.add_argument("-m", "--method", help="export or import", required=True)
    parser.add_argument("-t", "--txt", help="txt file location", required=False)
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


def table_name_to_txt(table_name_list):
    now = datetime.now()

    dt_string = now.strftime("%Y-%d-%m %H:%M:%S")

    with open(r'hive_table_name_backup_' + dt_string + '.txt', 'w') as fp:
        for item in table_name_list:
            # write each item on a new line
            fp.write("%s\n" % item)
        logging.info('table name write into txt file done!!')
    fp.close()


def txt_to_table_name(file):
    a_file = open(file, "r")

    lines = a_file.read()
    return lines.splitlines()


def export_hive_data_to_hdfs(conn, table_name_list, hdfs_location):
    cursor = conn.cursor()
    for name in table_name_list:

        try:
            cursor.execute("export table " + name + " to " + "'" + hdfs_location + "/" + name + "'")
        except Exception as e:
            logging.error("execute export error: ", e)


def import_hdfs_data_to_hive(conn, table_name_list, hdfs_location):
    cursor = conn.cursor()
    for name in table_name_list:

        try:
            cursor.execute("import from  " + "'" + hdfs_location + "/" + name + "'")
        except Exception as e:
            logging.error("execute import error: ", e)


if __name__ == '__main__':
    args = init_argparse()
    conn = connect_to_hive(args.hiveHost, args.port, args.username, args.database, args.passwd)

    if args.method == "export":

        table_name = get_hive_table(conn)
        export_hive_data_to_hdfs(conn, table_name, args.hdfsAddress)
        table_name_to_txt(table_name)
    elif args.method == "import":
        table_name = txt_to_table_name(args.txt)
        import_hdfs_data_to_hive(conn, table_name, args.hdfsAddress)
    else:
        logging.error("method not support!")
    conn.close()
