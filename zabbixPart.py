#   ----------------------------------------------------------------------------
#   "THE BEER-WARE LICENSE" (Revision 42):
#   misiek.rutkowski@gmail.com wrote this file. As long as you retain this notice you
#   can do whatever you want with this stuff. If we meet some day, and you think
#   this stuff is worth it, you can buy me a beer in return.
#   ----------------------------------------------------------------------------
 

import mysql.connector
import datetime
import sys

dbUser = 'zabbix'
dbName = 'zabbix'
dbPass = 'password'

# function to partition existing zabbix tables
def partition(tablename, interval):
    db = mysql.connector.connect(user=dbUser, database=dbName, password=dbPass)
    cursor = db.cursor(buffered=True)
    print("hello i am going to partition "+tablename+" with interval of "+str(interval)+" days")
    print("")
    print("create empty temp table with the same schema")
    tempTableName = tablename+"j23fj09awdj"

    print("first drop temp table if it exist")
    try:
        Query = "DROP TABLE "+tempTableName+";"
        print(Query)
        Query = (Query)
        cursor.execute(Query)
        db.commit()
    except:
        pass

    print("and create the temp table")
    Query = "CREATE TABLE "+tempTableName+" LIKE "+tablename+";"
    print(Query)
    Query = (Query)
    cursor.execute(Query)
    db.commit()

    print("get first day of the data")
    Query = ("SELECT FROM_UNIXTIME(MIN(clock)) FROM "+tablename+";")
    print(Query)
    cursor.execute(Query)
    for val in cursor:
        partTimestamp = int(val[0].timestamp())
        partDatetime = val[0]

    print("get last day of the data")
    Query = ("SELECT FROM_UNIXTIME(MAX(clock)) FROM "+tablename+";")
    print(Query)
    cursor.execute(Query)
    for val in cursor:
        print(val[0])
        maxTimestamp = int(val[0].timestamp())
        maxDatetime = val[0]
    unixTimestamp = str(partDatetime.year)+"-"+str(partDatetime.month).zfill(2)+"-"+str(partDatetime.day).zfill(2)+" 00:00:00"
    diff = maxDatetime - partDatetime

    print("start partitioning - create partition in temp table for "+str(diff.days)+" days")
    firstpart = True
    while diff.days > 0:
        partName = "p"+str(partDatetime.year)+str(partDatetime.month).zfill(2)+str(partDatetime.day).zfill(2)
        Query = ""
        if firstpart:
            Query = "ALTER TABLE "+tempTableName+" PARTITION BY RANGE(clock) PARTITIONS 1(PARTITION "+partName+" VALUES LESS THAN("+str(int(partDatetime.timestamp()))+"));"
            firstpart = False
        else:
            Query = "ALTER TABLE "+tempTableName+" ADD PARTITION (PARTITION "+partName+" VALUES LESS THAN("+str(int(partDatetime.timestamp()))+"));"
        partDatetime += datetime.timedelta(days=interval)
        diff = maxDatetime - partDatetime
        print("days left: "+str(diff.days))
        print(Query)
        Query = (Query)
        cursor.execute(Query)
        db.commit()

    print("last interval")
    maxDatetime = datetime.datetime(year=maxDatetime.year, month=maxDatetime.month, day=maxDatetime.day)
    partName = "p"+str(maxDatetime.year)+str(maxDatetime.month).zfill(2)+str(maxDatetime.day).zfill(2)
    Query = "ALTER TABLE "+tempTableName+" ADD PARTITION (PARTITION "+partName+" VALUES LESS THAN("+str(int(maxDatetime.timestamp()))+"));"
    Query = (Query)
    print(Query)
    cursor.execute(Query)
    db.commit()

    print("and one more to be sure....")
    maxDatetime += datetime.timedelta(days=interval)
    partName = "p"+str(maxDatetime.year)+str(maxDatetime.month).zfill(2)+str(maxDatetime.day).zfill(2)
    Query = "ALTER TABLE "+tempTableName+" ADD PARTITION (PARTITION "+partName+" VALUES LESS THAN("+str(int(maxDatetime.timestamp()))+"));"
    print(Query)
    Query = (Query)
    cursor.execute(Query)
    db.commit()

    print("copy data to temp table - this might take a while")
    Query = "INSERT INTO "+tempTableName+" SELECT * FROM "+tablename+";"
    print(Query)
    Query = (Query)
    cursor.execute(Query)
    db.commit()

    print("drop old table")
    Query = "DROP TABLE "+tablename+";"
    print(Query)
    Query = (Query)
    cursor.execute(Query)
    db.commit()

    print("rename temp table")
    Query = "ALTER TABLE "+tempTableName+" RENAME TO "+tablename+";"
    print(Query)
    Query = (Query)
    cursor.execute(Query)
    db.commit()
    print("all done!")
    cursor.close()
    db.close()

#function to create day to day partitions, one for next day and remove old empty partitions
def dailyRoutine(tablename, interval):
    db = mysql.connector.connect(user=dbUser, database=dbName, password=dbPass)
    cursor = db.cursor(buffered=True)
    print("first delete oldest partitions if they are empty")
    Query = "SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME=\'"+tablename+"\' AND TABLE_SCHEMA=\'"+dbName+"\' order by PARTITION_ORDINAL_POSITION asc;"
    print(Query)
    Query = (Query)
    cursor.execute(Query)
    result = cursor.fetchall()
    for partName in result:
        Query = "SELECT COUNT(*) FROM "+tablename+" PARTITION("+partName[0]+");"
        print(Query)
        Query = (Query)
        cursor.execute(Query)
        result = cursor.fetchall()
        if result[0][0] == 0:
            print("removing empty partition "+partName[0])
            Query = "ALTER TABLE "+tablename+" DROP PARTITION "+partName[0]+";"
            print(Query)
            Query = (Query)
            cursor.execute(Query)
            db.commit()
        else:
            break

    print("adding partitions for "+tablename)
    print("checking last partition")
    Query = "SELECT FROM_UNIXTIME(PARTITION_DESCRIPTION) FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME=\'"+tablename+"\' AND TABLE_SCHEMA=\'"+dbName+"\' order by PARTITION_ORDINAL_POSITION desc;"
    print(Query)
    Query = (Query)
    cursor.execute(Query)
    result = cursor.fetchall()
    lastPart = result[0][0]
    if lastPart - datetime.datetime.now() < datetime.timedelta(days=interval):
        print("it is a good time to create a new partition for "+tablename)
        lastPart += datetime.timedelta(days=interval)
        partName = "p"+str(lastPart.year)+str(lastPart.month).zfill(2)+str(lastPart.day).zfill(2)
        Query = "ALTER TABLE "+tablename+" ADD PARTITION (PARTITION "+partName+" VALUES LESS THAN("+str(int(lastPart.timestamp()))+"));"
        Query = (Query)
        print(Query)
        cursor.execute(Query)
        db.commit()
    else:
        print("partitions are ok")
    cursor.close()
    db.close()


if len(sys.argv) < 4 or sys.argv[1] == "help":
    print("usage: zabbixPart [interval] [tablename] [dbname] [login] [pass] [init]")
    print("[interval] = in days, how big should partitions be")
    print("[tablename] = name of the zabbix table")
    print("[dbname] = name of the zabbix database")
    print("[login] = login to the zabbix database")
    print("[password] = password to the zabbix database")
    print("[init] = OPTIONAL, pass \"init\" to initialize the partitioning if you have not done it already")
    print("")
    print("[interval] - for [init] partitioning, the interval can be as long as you want")
    print("but for day to day partitions you can change it, but remember,")
    print("the data need to have a partition!")
    print("[tablename] - should be a table which contains a \"clock\" column")
    print("for example: trends, trends_uint, history, history_log, etc...")
    print("[init] - remember that you can always repartition partitioned table")
    print("for example when you want to change the interval")
else:
    interval = sys.argv[1]
    tablename = sys.argv[2]
    dbName = sys.argv[3]
    dbLogin = sys.argv[4]
    dbPass = sys.argv[5]
    print("interval: "+str(interval)+" days")
    print("tablename: "+tablename)

    if len(sys.argv) > 4:
        if sys.argv[6] == "init":
            print("Starting partitioning tables with data and trends")
            partition(tablename, int(interval))
    else:
        dailyRoutine(tablename, int(interval))