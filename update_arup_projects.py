# update arup projects table
# Author: YANGYANG CAI
# Date  : 22/06/2015

# -*- coding: utf-8 -*-
#Import the modules
import pymysql 
import sys
from datetime import datetime
import logging

# Update arup_project table 
def main():
    try:
        # Connect to MySQL database
        #con = MySQLdb.connect(host="localhost",user="root",passwd='1983',db="ba3")
        #cur = con.cursor()
        con = pymysql.connect(host='localhost',
                                     user='root',
                                     password='1983',
                                     db='ba3'
                                     )
        cur=con.cursor()
         # clean arup_projects first
        cur.execute("Set SQL_SAFE_UPDATES = 0")
        cur.execute("truncate table arup_projects")

        sql = r"""insert into arup_projects (mod_id,project_number, arup_projects_number) select distinct mod_id, project_number, arup_projects_number from 
                    (select mod_id,
                        case length(split_str(mod_path,'\\', 7)) 
                            when  1 then substr(concat(split_str(mod_path, '\\',7),split_str(mod_path,'\\' ,8)),1, 9) 
                            when  6 then substr(split_str(mod_path, '\\',8),1,9) 
                            END as project_number, 
                        case length(split_str(mod_path,'\\', 7)) 
                            when 1 then concat(split_str(substr(concat(split_str(mod_path,'\\' ,7),split_str(mod_path,'\\' ,8)),1, 9),'-', 1), split_str(substr(concat(split_str(mod_path, '\\', 7),split_str(mod_path, '\\' ,8)),1, 9),'-', 2)) 
                            when 6 then concat(split_str(substr(split_str(mod_path,'\\',8),1,9),'-',1), split_str(substr(split_str(mod_path,'\\',8),1,9),'-',2)) 
                            END as arup_projects_number 
                    from bamodels) as project_number 
                order by mod_id"""

        # update arup_projects first
        cur.execute(sql)
        cur.execute("commit")
        print("Arup Projects have been updated!")

        # Logging file
        logname = "log_history.txt"
        logging.basicConfig(filename = logname, filemode='a', level=logging.DEBUG,format = "%(asctime)s %(levelname)s %(message)s", datefmt = '%d-%m-%Y %H:%M:%S')
        logging.info('project details updated\n')

    except pymysql.Error as e:
        print("Error %d: %s" % (e.args[0],e.args[1]))
        sys.exit(1)  
    finally:          
        if con:    
            con.close()

if __name__ == "__main__":
    main()