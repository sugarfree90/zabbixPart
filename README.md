# zabbixPart
Partitioner for zabbix mysql database with ability to partition existing database.
It allows you to partition existing mysql zabbix database when run with "init" parameter.
In order to partition existing zabbix it will:
1. Create temporary table with structure of the original
2. Check the range of the data (time data from the first and last entry)
3. Create partition in temp table for each interval (default 30 days for trends and 1 day for history)
4. Copy data from original table to the partitioned temp table
5. Drop the original table
6. Rename the temp table so it will become the original table, but partitioned

Without the "init" parameter, the program will add day to day partitions and remove old empty ones
1. It will check for all partitions that were created for given table
2. As long as it will find empty partitions (starting from the oldest) it will drop those partitions, this is so if zabbix garbage collector will remove old data, the partition will also get removed
3. Check wether the time to the end of the last partition is less than interval
4. If so, create partition

## Requirements if you want to use python version
1. Install mysql.connector
    ```
    sudo pip3 install mysql.connector
    ```
## Usage
    ```
    zabbixPart [interval] [tablename] [dbname] [login] [pass] [init]
    ```
or if you want to use the uncompiled version
    ```
    python3 zabbixPart.py [interval] [tablename] [dbname] [login] [pass] [init]
    ```
## Example
### Partition existing zabbix database
    ```
    zabbixPart 7 history zabbixDatabase zabbixDatabaseLogin zabbixDatabasePassword init
    ```
### Check if there is a need for a new partition - you can put it in Cron
    ```
    zabbixPart 7 history zabbixDatabase zabbixDatabaseLogin zabbixDatabasePassword
    ```
