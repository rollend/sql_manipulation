# Update Star Schema 
# Author: YANGYANG CAI
# Date  : 2/02/2015

# -*- coding: utf-8 -*-
import pymysql
import logging
import sys
import configparser
import traceback

def main():
    try:
        logname = "log_history.txt"
        logging.basicConfig(filename = logname, filemode='a', level=logging.DEBUG,format = "%(asctime)s %(levelname)s %(message)s", datefmt = '%d-%m-%Y %H:%M:%S')
        logging.info('Starting update of star schema.\n')
        
        config = configparser.ConfigParser()
        config.read("dbconfig.ini")
        print("Connecting to: " + config.get("mysql", "host") + " as user: " + config.get("mysql", "user"))

        # Connect to MySQL database
        con = pymysql.connect(host=config.get("mysql", "host"),user=config.get("mysql", "user"),passwd=config.get("mysql", "passwd"),db=config.get("mysql", "db"),charset='utf8')
        cur = con.cursor()
        
        rf = open("star_schema_ver_2.0.sql","r")
        sql_file = rf.read()
        queries = (sql_file.split(";"))
        for query in queries:
            if query.strip():
                cur.execute(query)
                cur.execute("commit")
                print(query.encode('utf8').decode(sys.stdout.encoding))
        print("star schema has been updated!")

        
        logging.info('star schema has been updated!\n')
 
    except pymysql.Error as e:
        print("Error %d: %s" % (e.args[0],e.args[1]))
        logging.exception("Error updating star schema")
        
        sys.exit(1)
    except:
        e = sys.exc_info()[0]
        print("Error %s: " % e)
        sys.exit(1)
    finally:          
        if con:    
            con.close()

if __name__ == "__main__":
    main()
    
