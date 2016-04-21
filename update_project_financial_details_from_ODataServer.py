# Caching project detail data from OData web service 
# Author: YANGYANG CAI
# Date  : 16/02/2015

# -*- coding: utf-8 -*-
# Import the modules
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

project_details_sqls = []

# url query example
# https://arupdatahub.arup.com/Adh.svc/Projects('08313100')?$select=ProjectCode,AccountingCentre/AccountingCentreNameLong,AccountingCentre/Group/GroupCode,AccountingCentre/Group/GroupName,JobStatus,JobType,OrganisationName,CountryName,Town,StartDate,EndDate,LastInvoiceDate,SalaryCostAUD,OverheadsAUD,FeesReceivedAUD,ExpensesReceivedAUD,%20BadDebtsAUD,%20ForecastFeesAUD,ForecastExpenseIncomeAUD,ForecastSalaryCostAUD,ForecastOverheadsAUD,PercentCompleteAUD,ProfitFeesIncomeAUD,ProfitGrossExpensesAUD,ProfitExpensesIncomeAUD,ProfitIncomeAUD,ProfitCostsAUD,ProfitAUD,ProfitPctAUD,CashCostsAUD,CashFlowAUD,ForecastStaffRelOverheadsAUD,ForecastStaffRelOverheadsAUD,StaffRelatedOverheadsAUD,ProjectRelatedOverheadsAUD,ForecastGrossExpExclContAUD,ForecastContingencyAUD,ForecastFeesInvoicedAUD,ForecastExpensesInvoicedAUD,SalaryCostGBP,OverheadsGBP&$expand=AccountingCentre,AccountingCentre/Group&$filter=ProjectCode%20eq%20%2720548500%27&$format=json

#Can update this:
#https://arupdatahub.arup.com/Adh.svc/Projects?$select=JobStatus&$filter=ProjectCode%20eq%20%2720548500%27&$format=json
#to
#https://arupdatahub.arup.com/Adh.svc/Projects('08313100')?$select=JobStatus&$format=json

# Generate sqls for project 
def generate_sql(project_number):
    # Datahub wants 8 digit numbers project_numbers at least
    baseUrl = "https://arupdatahub.arup.com/Adh.svc/Projects('" + format(project_number, '08d') + "')?"
    url_query = "$select=ProjectCode,AccountingCentre/AccountingCentreNameLong,JobStatus,JobType,OrganisationName,CountryName,Town,StartDate,EndDate,LastInvoiceDate,SalaryCostAUD,OverheadsAUD,FeesReceivedAUD,ExpensesReceivedAUD,%20BadDebtsAUD,%20ForecastFeesAUD,ForecastExpenseIncomeAUD,ForecastSalaryCostAUD,ForecastOverheadsAUD,ProfitFeesIncomeAUD,ProfitGrossExpensesAUD,ProfitExpensesIncomeAUD,ProfitIncomeAUD,ProfitCostsAUD,ProfitAUD,ProfitPctAUD,CashCostsAUD,CashFlowAUD,ForecastStaffRelOverheadsAUD,ForecastStaffRelOverheadsAUD,StaffRelatedOverheadsAUD,ProjectRelatedOverheadsAUD,ForecastGrossExpExclContAUD,ForecastContingencyAUD,ForecastFeesInvoicedAUD,ForecastExpensesInvoicedAUD,SalaryCostGBP,OverheadsGBP"
    url_expand = "&$expand=AccountingCentre,AccountingCentre/Group"
    url_filter = ""
    url_format = "&$format=json"
    print(project_number)
    # Get the feed
    r = requests.get( baseUrl + url_query + url_expand + url_filter + url_format,auth=("steven.downing@arup.com","tV2B58HR"))        # This password is not linked to Steve's normal account

    print(r.text)
    # Load data to unicode dictory
    data = json.loads(r.text)

    # Loop throught the result into project details dictory (format data)
    dic = {}
    project_details = {}
    for key, value in data.items():
        #print(key)
        #key = unicodedata.normalize("NFKD",key).encode("ascii","ignore")
        # if key == 'value':
        #     for item in value:
        #         for key,value in item.items():
        #key = unicodedata.normalize("NFKD",key).encode("ascii","ignore")
        if (value is not None) and ( "odata." not in key):
            if key == "ProjectCode":
                project_details["project_number"] = project_number
            elif key == "OrganisationName":
                #value = unicodedata.normalize("NFKD",value).encode("ascii","ignore")
                value = value.replace("'","`")
                project_details["organisationName"] = value
            elif key == "AccountingCentre":
                for key,value in value.items():
                    #value = unicodedata.normalize("NFKD",value).encode("ascii","ignore")
                    value = value.replace("'","`")
                    project_details['accountingCentreNameLong'] = value
            else:
                #content = unicodedata.normalize("NFKD",value).encode("ascii","ignore")
                project_details[key] = value #.replace("\n","")

    for key, value in project_details.items():
        print(key)
        print(value)
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
        con = pymysql.connect(host=config.get("mysql", "host"),user=config.get("mysql", "user"),passwd=config.get("mysql", "passwd"),db=config.get("mysql", "db"),charset='utf8')
        cur = con.cursor()

        # Get all project id list from MySQL database
        cur.execute("select distinct arup_projects_number from arup_projects")
        raw_list = []
        all_project_list = []
        for row in cur:
            raw_list.append(list(row))
        for i in raw_list:
            all_project_list.append(int(str(i)[1:-1]))

        # Get sqls for each project number
        for i in all_project_list:
            generate_sql(i)

        # Update project details table
        for update_sql in project_details_sqls:
            print (update_sql.encode('utf8').decode(sys.stdout.encoding))
            if update_sql.strip():
                print (update_sql.encode('utf8').decode(sys.stdout.encoding))
                cur.execute(update_sql)
                cur.execute("commit")
                
        print ("Project financial details have been updated!")

        # Logging file
        logname = "log_history.txt"
        logging.basicConfig(filename = logname, filemode='a', level=logging.DEBUG,format = "%(asctime)s %(levelname)s %(message)s", datefmt = '%d-%m-%Y %H:%M:%S')
        logging.info('Project financial details have been updated\n')

    except pymysql.Error as e:
        print("Mysql error %d: %s" % (e.args[0],e.args[1]))
        sys.exit(1)
    except:
        e = sys.exc_info()[0]
        #print(e)
        #print ("Error %s: " % e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback)
        sys.exit(1)
    finally:          
        if con:    
            con.close()

if __name__ == "__main__":
    main()
