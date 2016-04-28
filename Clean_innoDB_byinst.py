# -*- coding: utf-8 -*-
"""
Created on Tue Apr 19 09:46:18 2016

@author: Dr. Shen.Xu
Due innoDB delete query performance issue, which database can be easily lockup even in small chunks. 
Alternative solution is create a new table to retain the wanted records. 

General sqls are as following:
create table new_table like old_table;
insert into new_table (select * from old_table where <what you want to keep>)
rename table old_table to old_table_drop_prep, new_table to old_table;
drop table old_table_drop_prep;

Tested but doesnt perform well, 2 million rows requires at least 2 hrs. 

Suggested General steps:
    Create parameters for filtering duplicates using dictionary 
    Find duplicates for each mod id, sync id respectively
    Validate (Validation process is checking the found duplicate instances that existing in the cleaned instances dataset)
    Create a new table with sufix _new (only once) and insert ignore into new table
    delete unwanted pi_id from new table, loop for all sync ids through single commiting
    Final check of instance number matches    
    !!Rename table: Waiting for table metadata lock easily
    Alternative: Final confirm then perform on the original table
    Drop unwanted table
    
Note:
    Current recognized mapping pattern for params_inst: 
        pi_type_id = inst_id aggregated by inst_psid
    
    MySQL workbench was reporting Index out of range when select * from new table when performing insert, 
    closing the connections from this script and restart MySQL workbench resolved the problem. 

How to use: Specify database information in dbconfig.ini first then
            Specify which table you want to clean (currently only params_inst)            
            type main(database,table)
            Select option to perform, suggested process is practice first if there is enough space 
            
"""

import pymysql
import pandas as pd
import sys
import configparser
import traceback
import numpy as np

def connect_database(database):
    try:
            #print ("Reading config from .\dbconfig.ini")
            config = configparser.ConfigParser()
            config.read("dbconfig.ini")
            host=config.get("%s" %database, "host") 
            user=config.get("%s"%database, "user")   
            passwd=config.get("%s"%database, "passwd")   
            db=config.get("%s"%database, "db")   
            charset='utf8'         
            #print("Connecting to: " + host + " as user: " + user)            
            
            # Connect to MySQL database
            con = pymysql.connect(host=host,user=user,passwd=passwd,db=db,charset=charset)
            #cur1 = con.cursor()
            #cur2 = con.cursor()
            #cur1.execute("select * from arup_projects")
            #projects=pd.read_sql("select * from arup_projects",con)
            #print(projects)
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
            print("Contiuening")
    return host,db,user,passwd,db,charset
    
def duplicate_parameters_lookingup(database):
    #host,db,user,passwd,db,charset=connect_database(database)
    #con = pymysql.connect(host=host,user=user,passwd=passwd,db=db,charset=charset)
    #table=pd.read_sql("SELECT * FROM INFORMATION_SCHEMA.Columns where TABLE_SCHEMA = '%s'" %db,con)
    duplicates_parameters={}
    duplicates_parameters['params_inst']=['pi_type_id', \
                            'pi_name', \
                            'pi_numeric',\
                            'pi_storage_type',\
                            'pi_parameter_type',\
                            'pi_group_name',\
                            'pi_text',\
                            'pi_readonly',\
                            'pi_display_unit_type']
    duplicates_parameters['params_type']=['pt_type_id', \
                            'pt_name', \
                            'pt_guid',\
                            'pt_storage_type',\
                            'pt_parameter_type',\
                            'pt_group_name',\
                            'pt_text',\
                            'pt_readonly',\
                            'pt_display_unit_type']
    return duplicates_parameters

#def table_mapping():
    
    
def find_duplicates(database,inst_id,table):
    host,db,user,passwd,db,charset=connect_database(database)
    con=pymysql.connect(host=host,user=user,passwd=passwd,db=db,charset=charset,connect_timeout=2000)
    duplicates_parameters=duplicate_parameters_lookingup(database)
    
    #Based on table set criteria                            
    #params_inst_all=pd.read_sql("""SELECT * FROM %s where pi_type_id IN
                                #(select inst_id 
                                        #from elem_inst
                                        #where inst_psid = %s)""" %(table,sync),con)
    params_inst_all=pd.read_sql("""SELECT * FROM %s where pi_type_id in (%s)""" %(table,inst_id),con)    
    p_clean=params_inst_all[~params_inst_all.duplicated(duplicates_parameters[table])]
    p_duplicate=params_inst_all[params_inst_all.duplicated(duplicates_parameters[table])]
    con.close()    
    return p_clean, p_duplicate, duplicates_parameters
    
def validating(p_clean, p_duplicate, duplicates_parameters,table):
    #Validation processes come through two parts, criteria should be the same
    #1. SQL query should get the same number of instances but will result different pi_id (too expensive, not implemented)
    #2. All instances in the duplicates dataset should find same one instance (except pi_id) in the cleaned dataset both from Panda or SQL    
    
    if p_duplicate.empty:       
       print("Validate success-No duplicates")
       return p_duplicate
    else:                
        assertion_list=list()
        assertion_list.append("len(p_duplicate[~(")
        i=0    
        while i < len(duplicates_parameters[table])-1:
            assertion_list.append("p_duplicate.%s.isin(p_clean.%s)|" %(duplicates_parameters[table][i],duplicates_parameters[table][i]))
            i=i+1
        assertion_list.append("p_duplicate.%s.isin(p_clean.%s))])==0" %(duplicates_parameters[table][i],duplicates_parameters[table][i]))
        a=''.join(assertion_list)
        assert(a)
           
        print("Validate success")
        return p_duplicate
def copy_database(database,table):
    host,db,user,passwd,db,charset=connect_database(database)
    con=pymysql.connect(host=host,user=user,passwd=passwd,db=db,charset=charset)    
    sql_create_table = """create table IF NOT EXISTS %s_new like %s""" %(table,table)
    sql_copy_table= """insert ignore into %s_new select * from %s """ %(table,table)
    cursor=con.cursor()
    cursor.execute(sql_create_table)
    cursor.execute("commit")
    cursor.execute(sql_copy_table)
    cursor.execute("commit")
    con.close()
                                    
def commit_to_database(p_duplicate,database,table):    
    host,db,user,passwd,db,charset=connect_database(database)
    con=pymysql.connect(host=host,user=user,passwd=passwd,db=db,charset=charset,connect_timeout=2000)
    cursor=con.cursor()
    #sql_insert_id=[]
    #for id in p_clean.pi_id.values:
        #sql_insert_noneduplicates= """insert ignore into params_inst_new (select * from params_inst where pi_id = %s)""" % (id)
        #sql_id.append(sql_insert_noneduplicates) 
    sql_delete_id=[]
    if p_duplicate.empty:
        print("None has been deleted")
        con.close()
    else:
        for id in p_duplicate.pi_id.values:
            sql_delete_duplicates= """delete from params_inst_new where pi_id = %s""" % (id)
            sql_delete_id.append(sql_delete_duplicates)
        for delete_id in sql_delete_id:
            print (delete_id.encode('utf8').decode(sys.stdout.encoding))
            cursor.execute(delete_id)
            cursor.execute("commit")
        con.close()

def commit_to_org_database(p_duplicate,database,table):    
    host,db,user,passwd,db,charset=connect_database(database)
    con=pymysql.connect(host=host,user=user,passwd=passwd,db=db,charset=charset,connect_timeout=2000)
    cursor=con.cursor()
    #sql_insert_id=[]
    #for id in p_clean.pi_id.values:
        #sql_insert_noneduplicates= """insert ignore into params_inst_new (select * from params_inst where pi_id = %s)""" % (id)
        #sql_id.append(sql_insert_noneduplicates) 
    sql_delete_id=[]
    if p_duplicate.empty:
        print("None has been deleted")
        con.close()
    else:
        for id in p_duplicate.pi_id.values:
            sql_delete_duplicates= """delete from %s where pi_id = %s""" % (table,id)
            sql_delete_id.append(sql_delete_duplicates)
        for delete_id in sql_delete_id:
            cursor=con.cursor()
            print (delete_id.encode('utf8').decode(sys.stdout.encoding))
            cursor.execute(delete_id)
            cursor.execute("commit")
            cursor.close()
        con.close()
"""        
def rename_table(database,table):
    host,db,user,passwd,db,charset=connect_database(database)
    con = pymysql.connect(host=host,user=user,passwd=passwd,db=db,charset=charset)
    
    sql_rename_table = "rename table %s\
                        to %s_drop_prep,\
                        %s_new \
                        to %s;" %(table,table,table,table)
    sql_rename_table_reverse= "rename table %s \
                                to %s_new,\
                                %s_drop_prep \
                                to %s;"%(table,table,table,table)
     
    cursor=con.cursor()
    cursor.execute(sql_rename_table)
    cursor.execute("commit")
    con.close()

def drop_table(database,table):
    host,db,user,passwd,db,charset=connect_database(database)
    con = pymysql.connect(host=host,user=user,passwd=passwd,db=db,charset=charset)
    sql_drop_table= "drop table %s_new" %table   
    cursor=con.cursor()
    cursor.execute(sql_drop_table)
    cursor.execute("commit")
    con.close()
"""
def main(database,table):
    #database='local'
    #database='UAT'
    #table='params_inst'
    print("Start......")
    sync=input("Please input which syn in num you want to process, for a full clean please type in 'Full': ")    
    host,db,user,passwd,db,charset=connect_database(database)
    con = pymysql.connect(host=host,user=user,passwd=passwd,db=db,charset=charset)
    sync_inf=pd.read_sql("SELECT sync_id,mod_id FROM basyncs",con)
    if sync=='Full':    
        sucess_list={}
        records=pd.DataFrame()
        conti_not= input("Warning! Type Yes to perform on original table; No to perform creating practice:")
        if conti_not == 'Yes':              
            for i in sync_inf['sync_id']:
                print("Sync ID %d" %i)
                instance_inf=pd.read_sql("select inst_id from elem_inst where inst_psid = %s" %i,con)
                duplicates_num=0 
                aa=instance_inf['inst_id'].values.tolist()
                for j in chunker(aa,10):
                    p_clean, p_duplicate, duplicates_parameters= find_duplicates(database,str(j).strip('[]'),table)
                    p_duplicate_validate = validating(p_clean, p_duplicate, duplicates_parameters,table)        
                    commit_to_org_database(p_duplicate_validate,database,table)
                    duplicates_num=duplicates_num+len(p_duplicate)
                    if not p_duplicate.empty:                    
                        records=records.append(p_duplicate)
                records.to_csv('%s_records_droped_%s.csv'%(table,i))
                sucess_list[i]=duplicates_num                
            print(sucess_list) 
            con.close()
        else:
            print("Practice and validating")
            print("Copying database......")
            try:        
                sql_drop_table= "drop table %s_new" %table   
                cursor=con.cursor()
                cursor.execute(sql_drop_table)
                cursor.execute("commit")
                cursor.close()
            except pymysql.err.InternalError:
                print("Table already existing!")
            copy_database(database,table)    
            for i in sync_inf['sync_id']:
                print("Sync ID %d" %i)
                instance_inf=pd.read_sql("select inst_id from elem_inst where inst_psid = %s" %i,con)
                aa=instance_inf['inst_id'].values.tolist()                
                duplicates_num=0                
                for j in chunker(aa,10):
                    p_clean, p_duplicate, duplicates_parameters= find_duplicates(database,str(j).strip('[]'),table)
                    p_duplicate_validate = validating(p_clean, p_duplicate, duplicates_parameters,table)        
                    commit_to_database(p_duplicate_validate,database,table)
                    duplicates_num=duplicates_num+len(p_duplicate)
                sucess_list[i]=duplicates_num
            instances=pd.read_sql("SELECT count(*) AS SUM FROM %s" %table,con)
            instances_new=pd.read_sql("SELECT count(*) AS SUM FROM %s_new" %table,con)
            assert(sum(sucess_list.values())==(instances['SUM'][0]-instances_new['SUM'][0]))
            print(sucess_list)
            con.close()
    elif any(sync_inf.sync_id==float(sync)):        
        sucess_list={}
        records=pd.DataFrame()
        conti_not= input("Warning! Type Yes to perform on original table:")
        if conti_not == 'Yes':            
            print("Sync ID %s" %sync)
            instance_inf=pd.read_sql("select inst_id from elem_inst where inst_psid = %s" %sync,con)
            duplicates_num=0 
            aa=instance_inf['inst_id'].values.tolist()
            for j in chunker(aa,10):
                p_clean, p_duplicate, duplicates_parameters= find_duplicates(database,str(j).strip('[]'),table)
                p_duplicate_validate = validating(p_clean, p_duplicate, duplicates_parameters,table)        
                commit_to_org_database(p_duplicate_validate,database,table)
                duplicates_num=duplicates_num+len(p_duplicate)
                if not p_duplicate.empty:                    
                    records=records.append(p_duplicate)
            records.to_csv('%s_records_droped_%s.csv'%(table,i))
            sucess_list[i]=duplicates_num                
            con.close()
        else:
            print("Practice and validating")
            print("Copying database......")
            try:        
                sql_drop_table= "drop table %s_new" %table   
                cursor=con.cursor()
                cursor.execute(sql_drop_table)
                cursor.execute("commit")
                cursor.close()
            except pymysql.err.InternalError:
                print("Table already existing!")
            copy_database(database,table)    
            print("Sync ID %s" %sync)
            instance_inf=pd.read_sql("select inst_id from elem_inst where inst_psid = %s" %sync,con)
            aa=instance_inf['inst_id'].values.tolist()                
            duplicates_num=0                
            for j in chunker(aa,10):
                p_clean, p_duplicate, duplicates_parameters= find_duplicates(database,str(j).strip('[]'),table)
                p_duplicate_validate = validating(p_clean, p_duplicate, duplicates_parameters,table)        
                commit_to_database(p_duplicate_validate,database,table)
                duplicates_num=duplicates_num+len(p_duplicate)
            sucess_list[i]=duplicates_num
            instances=pd.read_sql("SELECT count(*) AS SUM FROM %s" %table,con)
            instances_new=pd.read_sql("SELECT count(*) AS SUM FROM %s_new" %table,con)
            assert(sum(sucess_list.values())==(instances['SUM'][0]-instances_new['SUM'][0]))
            print(sucess_list)
            con.close()       
    else:
        print("Sync ID %s not in the database" %sync)
        return
        
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size)) 
    
if __name__ == "__main__":
    database=input("Please type in database: ")
    table=input("Please type in table: ")
    main(database,table)