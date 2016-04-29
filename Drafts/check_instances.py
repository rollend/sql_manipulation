# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 11:09:08 2016

@author: Shen.Xu
"""

import pymysql
import pandas as pd
import numpy as np

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

sql_params_inst="SELECT `inst_id`, \
`pi_type_id`,\
`pi_id`,\
`inst_type_id`\
, `inst_uniqueid`, \
`inst_eid`, \
`inst_loc_x`, \
`inst_loc_y`, \
`inst_loc_z`, \
`inst_category_name`, \
`inst_owner_id`, `inst_owner_name`, `inst_host_id`, `inst_host_name`, `inst_object_class`, `inst_workset`, `inst_design_option`, `inst_group`, `pi_name`, `pi_guid`, `pi_readonly`, `pi_text`, `pi_numeric`, `pi_storage_type`, `pi_group_name`, `pi_display_unit_type`, `pi_parameter_type` \
FROM elem_inst as EI \
inner join params_inst as PI on pi_type_id = EI.inst_id where EI.inst_psid in \
			(select sync_id \
             from basyncs \
             where source_name in \
				('Autodesk Revit 2015','Autodesk Revit 2016')) \
order by inst_eid, pi_name;"

sql_selectduplicatestodrop="Select PI.*,\
            count(PI.pi_name) as Totalnumber\
	From params_inst PI	\
	Where PI.pi_type_id IN\
		(select EI2.inst_id \
		 from elem_inst EI2\
		 inner join elem_type ET2 on EI2.inst_type_id = ET2.type_id\
		 where EI2.inst_psid in \
			(select sync_id \
             from basyncs \
             where source_name in \
				('Autodesk Revit 2015','Autodesk Revit 2016')))\
	group by PI.pi_type_id, PI.pi_name, PI.pi_numeric,PI.pi_storage_type,PI.pi_parameter_type,PI.pi_group_name,PI.pi_text, PI.pi_readonly,PI.pi_display_unit_type having count(*)>1"

sql_displaytheduplicates="select A.*, \
       B.Totalnumber \
from params_inst AS A inner join \
	(Select PI.*,\
            count(PI.pi_name) as Totalnumber \
	From params_inst PI	\
	Where PI.pi_type_id IN \
		(select EI2.inst_id \
		 from elem_inst EI2 \
		 inner join elem_type ET2 on EI2.inst_type_id = ET2.type_id \
		 where EI2.inst_psid in \
			(select sync_id \
             from basyncs \
             where source_name in \
				('Autodesk Revit 2015','Autodesk Revit 2016'))) \
	group by PI.pi_type_id, PI.pi_name, PI.pi_numeric,PI.pi_storage_type,PI.pi_parameter_type,PI.pi_group_name,PI.pi_text, PI.pi_readonly,PI.pi_display_unit_type having count(*)>1) AS B \
ON \
(A.pi_type_id=B.pi_type_id and A.pi_name=B.pi_name and A.pi_group_name=B.pi_group_name and A.pi_numeric =B.pi_numeric and A.pi_storage_type =B.pi_storage_type and A.pi_readonly=B.pi_readonly)"
#A.pi_text =B.pi_text, there is null type in pi_text that will cause group problem results in displayed duplicates less than selected duplicates to drop

sql_selectduplicatestodrop_2times="Select PI.*,\
            count(PI.pi_name) as Totalnumber\
	From params_inst PI	\
	Where PI.pi_type_id IN\
		(select EI2.inst_id \
		 from elem_inst EI2\
		 inner join elem_type ET2 on EI2.inst_type_id = ET2.type_id\
		 where EI2.inst_psid in \
			(select sync_id \
             from basyncs \
             where source_name in \
				('Autodesk Revit 2015','Autodesk Revit 2016')))\
	group by PI.pi_type_id, PI.pi_name, PI.pi_numeric,PI.pi_storage_type,PI.pi_parameter_type,PI.pi_group_name,PI.pi_text, PI.pi_readonly,PI.pi_display_unit_type having count(*)=2"

sql_selectduplicatestodrop_3times="Select PI.*,\
            count(PI.pi_name) as Totalnumber\
	From params_inst PI	\
	Where PI.pi_type_id IN\
		(select EI2.inst_id \
		 from elem_inst EI2\
		 inner join elem_type ET2 on EI2.inst_type_id = ET2.type_id\
		 where EI2.inst_psid in \
			(select sync_id \
             from basyncs \
             where source_name in \
				('Autodesk Revit 2015','Autodesk Revit 2016')))\
	group by PI.pi_type_id, PI.pi_name, PI.pi_numeric,PI.pi_storage_type,PI.pi_parameter_type,PI.pi_group_name,PI.pi_text, PI.pi_readonly,PI.pi_display_unit_type having count(*)=3"


params_inst_duplicatestodrop=pd.read_sql(sql_selectduplicatestodrop,connection)

params_inst_duplicatestodrop_check=pd.read_sql(sql_selectduplicatestodrop,connection)

params_inst_duplicatestodrop_2=pd.read_sql(sql_selectduplicatestodrop_2times,connection)

params_inst_duplicatestodrop_3=pd.read_sql(sql_selectduplicatestodrop_3times,connection)

params_inst_allduplicates=pd.read_sql(sql_displaytheduplicates,connection)

duplicates_not_foundin_allduplicates=params_inst_duplicatestodrop[~params_inst_duplicatestodrop.pi_id.isin(params_inst_allduplicates.pi_id)]

params_inst=pd.read_sql(sql_params_inst,connection)

params_inst_cleaned=params_inst[~params_inst.pi_id.isin(params_inst_duplicatestodrop.pi_id)]

sql1="""SELECT * FROM ba3.concreteinstancewithlevel"""

sql2="""SELECT * FROM ba3.concrete_instances"""

levelinstance=pd.read_sql(sql1,connection)

concreteinstance=pd.read_sql(sql2,connection)

checklist= concreteinstance[~concreteinstance.inst_id.isin(levelinstance.inst_id)].inst_id.values.tolist()

check_sql="""select distinct(pi_text) from params_inst
where pi_type_id in (%s) 
and pi_name='Family'""" %str(checklist).strip('[]')

check=pd.read_sql(check_sql,connection)

