# back_hive_data
 back hive data to another hdfs via hive import export command

## example

export
```shell

python3 main.py -i xxx.xx.xx.xx -p 10000 -u xxxx -d default -f /backup/testpy -m export
```

import
```shell
python3 main.py -i xx.xx.xx.xx -p 10000 -u ispong -d db1 -f /backup/testpy -m import -t hive_table_name_backup_2022-17-06\ 19:33:27.txt  
```

usage
```shell
usage: main.py [-h] -i HIVEHOST -p PORT -u USERNAME [-w PASSWD] -d DATABASE -f HDFSADDRESS -m METHOD [-t TXT]

options:
  -h, --help            show this help message and exit
  -i HIVEHOST, --hiveHost HIVEHOST
                        hive host ip address
  -p PORT, --port PORT  hive port
  -u USERNAME, --username USERNAME
                        hive username
  -w PASSWD, --passwd PASSWD
                        hive password
  -d DATABASE, --database DATABASE
                        database
  -f HDFSADDRESS, --hdfsAddress HDFSADDRESS
                        hadoop hdfs address
  -m METHOD, --method METHOD
                        export or import
  -t TXT, --txt TXT     txt file location

```
