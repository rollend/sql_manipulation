# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 09:06:28 2016

@author: Shen.Xu
"""

import pymysql
import pandas as pd

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='1983',
                             db='ba3'
                             )
"""
try:
    with connection.cursor() as cursor:
        sql = "SELECT * FROM ba3.params_inst;"
        cursor.execute(sql)
        result=cursor.fetchall()
        
cursor=connection.cursor()
"""        

sql = "SELECT * FROM ba3.params_inst;"

df=pd.read_sql(sql,connection)    

df.describe()