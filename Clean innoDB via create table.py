# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 10:30:31 2016

@author: Shen.Xu

Due innoDB delete query performance issue, which database can be easily lockup even in small chunks. 
Current suggested solution is create a new table to retain the wanted records. 
create table new_table like old_table;
insert into new_table (select * from old_table where <what you want to keep>)
rename table old_table to old_table_drop_prep, new_table to old_table;
drop table old_table_drop_prep;
"""

import pymysql
import pandas as pd
import sys

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='1983',
                             db='ba3'
                             )
cursor=connection.cursor()

sql_create_table = """create table IF NOT EXISTS params_inst_new like params_inst"""

cursor.execute(sql_create_table)

sql_id=[]
for id in params_inst_cleaned.pi_id.values:
    sql_insert_noneduplicates= """insert into params_inst_new (select * from params_inst where pi_id = %s)""" % (id)
    sql_id.append(sql_insert_noneduplicates)
    
for insert_id in sql_id:
    print (insert_id.encode('utf8').decode(sys.stdout.encoding))
    cursor.execute(insert_id)
    cursor.execute("commit")
    
sql_volume_before_groupbyinstid = """Select EI.inst_id,
                                            EI.inst_type_id, 
                                            PI.pi_id,
                                            PI.pi_numeric, 
                                            PI.pi_display_unit_type, 
                                            PI.pi_text, 
                                            PI.pi_storage_type, 
                                            count(EI.inst_id) 
                                    from params_inst PI
                                    inner join elem_inst EI on EI.inst_id = PI.pi_type_id
                                    where pi_name = 'Volume' and pi_type_id in
                                	(select distinct(EI2.inst_id) from elem_inst EI2
                                    	inner join elem_type ET2 on EI2.inst_type_id = ET2.type_id
                                    	where EI2.inst_psid in (select max(sync_id) from basyncs where mod_id = 2)
                                    	and EI2.inst_category_name in ('Structural Columns','Walls','Floors','Strucural Framing','Structural Foundations', 'Structural Beam Systems', 'Structural Loads','Structural Rebar')
                                    	and (ET2.type_family_name LIKE '%conc%' OR ET2.type_type_name LIKE '%conc%')
                                    UNION
                                    Select distinct(EI2.inst_id) from elem_inst EI2
                                    	inner join params_inst PI2 on EI2.inst_id = PI2.pi_type_id
                                    	where EI2.inst_psid in (select max(sync_id) from basyncs where mod_id = 2)
                                    	and EI2.inst_category_name in ('Structural Columns','Walls','Floors','Strucural Framing', 'Structural Foundations', 'Structural Beam Systems', 'Structural Loads','Structural Rebar')
                                    	and PI2.pi_name = 'Structural Material'
                                    	and PI2.pi_text LIKE '%conc%'
                                    	)
                                    group by EI.inst_id; """

sql_volume_cleaned = """Select EI.inst_id,EI.inst_type_id, PI.pi_id,PI.pi_numeric, PI.pi_display_unit_type, PI.pi_text, PI.pi_storage_type, count(EI.inst_id)  from params_inst_new PI
inner join elem_inst EI on EI.inst_id = PI.pi_type_id
where pi_name = 'Volume'
and pi_type_id in
	(select distinct(EI2.inst_id) from elem_inst EI2
	inner join elem_type ET2 on EI2.inst_type_id = ET2.type_id
	where EI2.inst_psid in (select max(sync_id) from basyncs where mod_id = 2)
	and EI2.inst_category_name in ('Structural Columns','Walls','Floors','Strucural Framing','Structural Foundations', 'Structural Beam Systems', 'Structural Loads','Structural Rebar')
	and (ET2.type_family_name LIKE '%conc%' OR ET2.type_type_name LIKE '%conc%')
    UNION
    Select distinct(EI2.inst_id) from elem_inst EI2
	inner join params_inst_new PI2 on EI2.inst_id = PI2.pi_type_id
	where EI2.inst_psid in (select max(sync_id) from basyncs where mod_id = 2)
	and EI2.inst_category_name in ('Structural Columns','Walls','Floors','Strucural Framing', 'Structural Foundations', 'Structural Beam Systems', 'Structural Loads','Structural Rebar')
	and PI2.pi_name = 'Structural Material'
	and PI2.pi_text LIKE '%conc%'
	)
group by EI.inst_id; """

sql_totalvolume_before="""Select EI.inst_psid,EI.inst_category_name, 
		sum(PI.pi_numeric) AS Cubic_Feet, EI.inst_design_option,
        count(Distinct(EI.inst_id))  
From params_inst PI
Inner join elem_inst EI ON EI.inst_id = PI.pi_type_id
Where pi_name = 'Volume'
And pi_type_id IN
	(select EI2.inst_id 
	from elem_inst EI2
	inner join elem_type ET2 on EI2.inst_type_id = ET2.type_id
	where EI2.inst_psid in (select sync_id from basyncs)
	and EI2.inst_category_name in ('Structural Columns','Walls','Floors','Strucural Framing','Structural Foundations', 'Structural Beam Systems', 'Structural Loads','Structural Rebar')
	and (ET2.type_family_name LIKE '%conc%' OR ET2.type_type_name LIKE '%conc%' )
    UNION    
    select EI2.inst_id 
    from elem_inst EI2
	inner join params_inst PI2 on EI2.inst_id = PI2.pi_type_id
	where EI2.inst_psid in (select sync_id from basyncs)
	and EI2.inst_category_name in ('Structural Columns','Walls','Floors','Strucural Framing', 'Structural Foundations', 'Structural Beam Systems', 'Structural Loads','Structural Rebar')
	and PI2.pi_name = 'Structural Material'
	and PI2.pi_text LIKE '%conc%'
	)
group by EI.inst_psid,EI.inst_design_option,EI.inst_category_name;"""

sql_totalvolume_after="""Select EI.inst_psid,EI.inst_category_name, 
		sum(PI.pi_numeric) AS Cubic_Feet, EI.inst_design_option,
        count(Distinct(EI.inst_id))  
From params_inst_new PI
Inner join elem_inst EI ON EI.inst_id = PI.pi_type_id
Where pi_name = 'Volume'
And pi_type_id IN
	(select EI2.inst_id 
	from elem_inst EI2
	inner join elem_type ET2 on EI2.inst_type_id = ET2.type_id
	where EI2.inst_psid in (select sync_id from basyncs)
	and EI2.inst_category_name in ('Structural Columns','Walls','Floors','Strucural Framing','Structural Foundations', 'Structural Beam Systems', 'Structural Loads','Structural Rebar')
	and (ET2.type_family_name LIKE '%conc%' OR ET2.type_type_name LIKE '%conc%' )
    UNION    
    select EI2.inst_id 
    from elem_inst EI2
	inner join params_inst PI2 on EI2.inst_id = PI2.pi_type_id
	where EI2.inst_psid in (select sync_id from basyncs)
	and EI2.inst_category_name in ('Structural Columns','Walls','Floors','Strucural Framing', 'Structural Foundations', 'Structural Beam Systems', 'Structural Loads','Structural Rebar')
	and PI2.pi_name = 'Structural Material'
	and PI2.pi_text LIKE '%conc%'
	)
group by EI.inst_psid,EI.inst_design_option,EI.inst_category_name;"""

volume_before=pd.read_sql(sql_volume_before_groupbyinstid,connection)

volume_cleaned=pd.read_sql(sql_volume_cleaned,connection)

totalvolume_before=pd.read_sql(sql_totalvolume_before,connection)

totalvolume_cleaned=pd.read_sql(sql_totalvolume_after,connection)

len(volume_cleaned[~volume_cleaned.inst_id.isin(volume_before.inst_id)])

(totalvolume_before.Cubic_Feet-totalvolume_cleaned.Cubic_Feet)/totalvolume_before.Cubic_Feet


    num=1    
    base_sql="""Select *,
                count(*) as Totalnumber
                FROM %s where pi_type_id IN
                (select inst_id 
                from elem_inst
                where inst_psid = %s)
                group by %s 
                having count(*) > %d""" %(table,sync,','.join(i for i in duplicates_parameters['params_inst']).strip("''"), num)
                
    params_inst_sql=pd.read_sql(base_sql,con)
    
sql_list={}
        for i in params_inst_num:
            sql = """Select *,
                    count(*) as Totalnumber
                    FROM params_inst where pi_type_id IN
                    (select inst_id 
                    from elem_inst
                    where inst_psid = %s)
                    group by %s 
                    having count(*) = %d""" %(sync,','.join(i for i in duplicates_parameters[table]).strip("''"), i)
            sql_list[i]=sql        
        num=0
        for i in sql_list:
            params_inst_sql_time=pd.read_sql(sql_list[i],con)
            nu=len(params_inst_sql_time)*(i-1)
            num=num+nu
        
            
        assert(num==len(p_duplicate)),\
        "There are some problems, there are %d" %len(p_duplicate)\
        +"duplicates found in Sync ID = %d " %sync\
        +"at Table Name:%s" %table\
        +"By Panda"\
        +", but SQL doesn't agree with that number with %d" %num
        assertion_list=list()
        assertion_list.append("len(p_duplicate[~(")
        i=0    
        while i < len(duplicates_parameters[table])-1:
            assertion_list.append("p_duplicate.%s.isin(params_inst_sql.%s)|" %(duplicates_parameters[table][i],duplicates_parameters[table][i]))
            i=i+1
        assertion_list.append("p_duplicate.%s.isin(p_clean.%s))])==0" %(duplicates_parameters[table][i],duplicates_parameters[table][i]))
        b=''.join(assertion_list)
        assert(b) 

if __name__ == "__main__":
    main()