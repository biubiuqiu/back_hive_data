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
