# Caching project detail data from web service
# Author: YANGYANG CAI
# Date  : 2/02/2015

# -*- coding: utf-8 -*-
#Import the modules
import pymysql 
import sys
import requests
import json
import ast
import unicodedata
from datetime import datetime
from decimal import *
import logging
import configparser
import traceback

project_number_sqls = []
project_details_sqls =[]

# Generate sqls for project
def generate_sql(project_number, project_already_cached):
    # Base url
    url = "http://nosso.projects.intranet.arup.com/services/?resource=project.detail&JN="
    format_string = "&format=json"

    # Get the feed
    r = requests.get(url + str(project_number) + format_string)
    r.text
    #print(r.text)

    # Load data to unicode dictory.
    data = json.loads(r.text)
    #print(type(data))
    
    # Loop through the result into project details dictory (format data).
    project_details = {};
    for key, value in data.items():
        #print(key)
        #key = unicodedata.normalize("NFKD",key).encode("ascii","ignore")
        #print(key)
        if key == "message": continue
        elif key == "SUMMARY_URL":
            #url = unicodedata.normalize("NFKD",value).encode("ascii","ignore") 
            project_number = int(value[74:]) #FIXME - This looks fragile!
            project_details["project_number"] = project_number
        elif key == "ALLOCATED_DATE":
            #date = unicodedata.normalize("NFKD",value).encode("ascii","ignore")
            allocated_date = str(datetime.strptime(value.lower(),'%B, %d %Y %H:%M:%S'))
            project_details[key] = allocated_date
        elif key == "LOCATION":
            location = json.dumps(value)
            location_dic = ast.literal_eval(location)
            latitude = str(location_dic["LATLON"]["LATITUDE"])
            longitude = str(location_dic["LATLON"]["LONGITUDE"])
            project_details["latitude"] = latitude[:29] # Mysql only holds varchar(30)
            project_details["longitude"] = longitude[:29]
        else:
            #print (value)
            #content = unicodedata.normalize("NFKD",value).encode("ascii","ignore")
            #print (content)
            content = value.replace("'","`")
            project_details[key] = content #.replace("\n","")

    for key, value in project_details.items(): 
        if key == "project_number" and not project_already_cached:
            sql = "insert into projectdetails (%s) values (%d);" % (key,value)
            project_number_sqls.append(sql)
        else:
            sql = "update projectdetails set %s = '%s' where project_number = %d;" % (key,value, project_number)
            project_details_sqls.append(sql)

# Update project details table in MySQL Databse
def main():
    try:
        print ("Reading config from .\dbconfig.ini")
        config = configparser.ConfigParser()
        config.read("dbconfig.ini")
        print("Connecting to: " + config.get("mysql", "host") + " as user: " + config.get("mysql", "user"))

        # Connect to MySQL database
        con = pymysql.connect(host=config.get("mysql", "host"),user=config.get("mysql", "user"),passwd=config.get("mysql", "passwd"),db=config.get("mysql", "db"), charset='utf8')
        cur1 = con.cursor()
        cur2 = con.cursor()

        # Get all project id list from MySQL database
        cur1.execute("select distinct arup_projects_number from arup_projects")
        raw_list = []
        all_project_list = []
        for row in cur1:
            raw_list.append(list(row))
        for i in raw_list:
            all_project_list.append(int(str(i)[1:-1]))

        # Get updated project id list from MySQl database
        cur2.execute("select distinct project_number from projectdetails")
        raw_list2 = []
        updated_project_list = []
        for row in cur2:
            raw_list2.append(list(row))
        for i in raw_list2:
            updated_project_list.append(int(str(i)[1:-1]))

        # Get need updated project id list 
        project_list = []
        for i in all_project_list:
            if i in all_project_list and i not in updated_project_list:
                project_list.append(int(str(i)))

        # Get sqls for each new project number
        for i in project_list:
            generate_sql(i,False)
            
        #get sqls for existing projects
        for i in updated_project_list:
            generate_sql(i,True)

        # Update project details table
        for insert_sql in project_number_sqls:
            if insert_sql.strip():
                print (insert_sql.encode('utf8').decode(sys.stdout.encoding))
                cur1.execute(insert_sql)
                cur1.execute("commit")
                
        for update_sql in project_details_sqls:
            if update_sql.strip():
                print (update_sql.encode('utf8').decode(sys.stdout.encoding))
                cur1.execute(update_sql)
                cur1.execute("commit")
                
        cur1.execute("Set SQL_SAFE_UPDATES = 1")
        print ("Project Details have been updated!")

        # Logging file
        logname = "log_history.txt"
        logging.basicConfig(filename = logname, filemode='a', level=logging.DEBUG,format = "%(asctime)s %(levelname)s %(message)s", datefmt = '%d-%m-%Y %H:%M:%S')
        logging.info('project details updated\n')

    except pymysql.Error as e:
        print("Error %d: %s" % (e.args[0],e.args[1]))
        sys.exit(1)
    except:
        e = sys.exc_info()[0]
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback)
        sys.exit(1)
    finally:          
        if con:    
            con.close()

if __name__ == "__main__":
    main()
