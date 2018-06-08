import os
import pandas as pd
from collections import Counter
import MySQLdb
import sqlalchemy
import numpy as np
import datetime



def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 200000:
        c.execute('START TRANSACTION;')
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
        # upload
        connection.commit()
        print("UPLOAD!")
        # initialize
        sql_transaction = []


data_dir ='data'
colnames = ['HMS','DELETED1','USER','DELETED2','LOG_TYPE','AUTH_CL','PROGRAM','VOD_TYPE', 'EPISODE','*']
path = os.path.join(os.getcwd(),data_dir)
sql_transaction = []
count = 0


Dset = os.listdir(path)[5]

#colnames.pop(1)
#del colnames[1]

#for Dset in os.listdir(path):

    num_lines = sum(1 for line in open(os.path.join(path, Dset)))

    for tmp in pd.read_csv(os.path.join(path, Dset), sep='\t', chunksize=1000000):
        tmp.columns =colnames
        df = tmp.drop(['DELETED1','DELETED2','*'], axis = 1 )
        df['YMD'] = df.index
        fined_df = df.reset_index(drop = True)
        print('----DATA IMPORT----')


        # connector
        connection = MySQLdb.connect(host ='163.152.184.98',
                         user='root',
                         password='1225')
        c = connection.cursor()


        #
        c.execute("CREATE DATABASE IF NOT EXISTS {};".format('CJ'))
        c.execute("USE {};".format('CJ'))

        table_names = Dset.split('.')[0]


        # add
        date =['-'.join([str(i)[0:4], str(i)[4:6], str(i)[6:8]]) for i in fined_df.YMD]
        time = [':'.join([str(i).zfill(6)[0:2], str(i).zfill(6)[2:4],str(i).zfill(6)[4:6]]) for i in fined_df.HMS]
        dt =[datetime.datetime.strptime(''.join([i,'T',j]), "%Y-%m-%dT%H:%M:%S") for i, j in zip(date, time)]
        print('----INDEXING DATETIME----')
        fined_df['date_time'] = dt



        # remove
        upload_df =fined_df.drop(['HMS','YMD'], axis=1)
        # na
        upload_df_no_NA = upload_df.dropna(axis=0)

        upload_df_no_NA.columns
        # make a table
        c.execute("CREATE TABLE IF NOT EXISTS {} (date_time TIMESTAMP , USER CHAR(32), LOG_TYPE VARCHAR(5), AUTH_CL VARCHAR(4),\
        PROGRAM CHAR(32), VOD_TYPE VARCHAR(9), EPISODE CHAR(32))".format(table_names))


        for example in upload_df_no_NA.values:
            sql= """INSERT INTO {} (date_time , USER , LOG_TYPE , AUTH_CL , PROGRAM , VOD_TYPE ,\
            EPISODE) VALUES ("{}","{}","{}","{}","{}","{}","{}")""".format(table_names, example[6],example[0],example[1],
                                                                           example[2], example[3],example[4],
                                                                           example[5])
            transaction_bldr(sql)
            count+=1
            if count % 100000 == 0:
                print('{}//{}'.format(count, num_lines))




        print('------{} Done------'.format(Dset))





