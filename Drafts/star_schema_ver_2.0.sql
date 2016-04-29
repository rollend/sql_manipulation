/********************************************
* Star Schema ver 2.0
* Modified by Sun Ju Choi
*********************************************/

Drop table if exists Project_DIM;
Drop table if exists Project_Mod_Sync_DIM;  
Drop table if exists RVTLink_DIM;  
Drop table if exists CADLink_DIM;
Drop table if exists View_DIM;
Drop table if exists Sheet_DIM;
Drop table if exists Level_DIM;
Drop table if exists CategoryGroup_DIM;
Drop table if exists Instance_DIM;
Drop table if exists Latest_CategoryGroup_DIM;
Drop table if exists Latest_Instance_DIM;
Drop table if exists ElemTypeInst_FACT;
Drop table if exists ModelSummary_FACT;

Create table Project_DIM  as Select distinct * From projectdetails;

Create table Project_Mod_Sync_DIM as
    Select distinct ap.arup_projects_number, bs.mod_id, bs.sync_id, bs.sync_start, bs.sync_end, bs.sync_mb_start, bm.mod_name
    From arup_projects ap, basyncs bs, bamodels bm
    Where ap.mod_id = bs.mod_id
    and ap.mod_id = bm.mod_id;

Create table RVTLink_DIM as
	SELECT bs.mod_id, bs.sync_id, 
       et.type_uniqueid, et.type_category_name, et.type_family_name, et.type_type_name, et.type_object_class
	FROM basyncs bs, elem_Type et
	WHERE bs.sync_id = et.type_psid
	AND et.type_object_class = 'RevitLinkType';

Create table CADLink_DIM as
	SELECT bs.mod_id, bs.sync_id, 
       et.type_uniqueid, et.type_category_name, et.type_family_name, et.type_type_name, et.type_object_class
	FROM basyncs bs, elem_Type et
	WHERE bs.sync_id = et.type_psid
	AND et.type_object_class = 'CADLinkType';

Create table View_DIM as 
	Select bs.mod_id, bs.sync_id, et.type_uniqueid, et.type_category_name, et.type_family_name, et.type_type_name, et.type_object_class,
    ei.inst_uniqueid, ei.inst_category_name, ei.inst_object_class, ei.inst_owner_id, ei.inst_owner_name, ei.inst_workset, pi.pi_text as viewName
    From basyncs bs ,  elem_type et, elem_inst ei,  params_inst pi
    Where bs.sync_id = ei.inst_psid
	And ei.inst_type_id = et.type_id
	And et.type_object_class = 'ViewFamilyType'
	And et.type_family_name <> 'Legend'
	And et.type_family_name <> 'Schedule'
	AND ei.inst_category_name = 'Views'
	AND ei.inst_object_class <> 'Element'
	AND ei.inst_owner_name = ''  
	AND ei.inst_id = pi.pi_type_id
	And pi.pi_name  = 'View Name';

Create table Sheet_DIM as 
	Select bs.mod_id, bs.sync_id, et.type_uniqueid, et.type_category_name, et.type_family_name, et.type_type_name, 
    ei.inst_uniqueid, ei.inst_category_name, ei.inst_object_class, ei.inst_owner_id, ei.inst_owner_name, ei.inst_workset, pi1.pi_text as sheetName, pi2.pi_text as sheetNumber
    From basyncs bs, elem_type et, elem_inst ei,  params_inst pi1, params_inst pi2
    Where bs.sync_id = ei.inst_psid
    And ei.inst_type_id = et.type_id
    And ei.inst_id = pi1.pi_type_id
    And ei.inst_id = pi2.pi_type_id
    And ei.inst_category_name = 'Sheets'
    And pi1.pi_name = 'Sheet Name'
    and pi2.pi_name = 'Sheet Number';

Create table Level_DIM as
	Select bs.mod_id, bs.sync_id, et.type_uniqueid, et.type_category_name, et.type_family_name, et.type_type_name, 
    ei.inst_uniqueid, ei.inst_category_name, ei.inst_object_class, ei.inst_owner_id, ei.inst_owner_name, ei.inst_workset, pi.pi_text as levelName
    From basyncs bs, elem_type et, elem_inst ei, params_inst pi
    Where bs.sync_id = ei.inst_psid
    And et.type_id = ei.inst_type_id
    And ei.inst_id = pi.pi_type_id
    And ei.inst_object_class = 'Level'
    And pi.pi_name = 'Name'; 

Create table CategoryGroup_DIM as
    Select bs.mod_id, bs.sync_id, et.type_uniqueid, et.type_category_name, et.type_family_name, et.type_type_name,et.type_object_class
    From basyncs bs, elem_type et
    Where bs.sync_id = et.type_psid;

Create table Instance_DIM as
    select bs.mod_id, bs.sync_id, et.type_uniqueid, et.type_category_name, et.type_family_name, et.type_type_name,
    ei.inst_uniqueid, ei.inst_category_name, ei.inst_object_class, ei.inst_owner_id, ei.inst_owner_name, ei.inst_workset
    from elem_inst ei
    left join basyncs bs on bs.sync_id = ei.inst_psid
    left join elem_type et on bs.sync_id = et.type_psid and et.type_id = ei.inst_type_id;

Create table Latest_CategoryGroup_DIM as
    select * from categorygroup_dim where sync_id in (select max(sync_id) from basyncs group by mod_id);

 Create table Latest_Instance_DIM as
	select * from instance_dim where sync_id in (select max(sync_id) from basyncs group by mod_id);
  
Create table ElemTypeInst_FACT as
    select pmsd.arup_projects_number, lcd.mod_id, lcd.sync_id, pmsd.mod_name as model_name, pmsd.sync_end, lcd.type_category_name as category , lcd.type_family_name as family, lcd.type_type_name as type, lcd.type_uniqueid
    from project_mod_sync_dim pmsd, latest_categorygroup_dim lcd
    where pmsd.sync_id = lcd.sync_id ;  
    
Alter table ElemTypeInst_FACT add families int(255);
Alter table ElemTypeInst_FACT add family_types int(255);
Alter table ElemTypeInst_FACT add category_types int(255);
Alter table ElemTypeInst_FACT add categoryInstance int(255);
Alter table ElemTypeInst_FACT add familyInstance int(255);
Alter table ElemTypeInst_FACT add typeInstance int(255);

SET SQL_SAFE_UPDATES = 0;

update elemtypeinst_fact ef
inner join (select sync_id, type_category_name, count(distinct(type_family_name)) as families from latest_categorygroup_dim group by sync_id, type_category_name) as cf 
on ef.sync_id = cf.sync_id 
and ef.category = cf.type_category_name
set ef.families = ifnull(cf.families,0);

update elemtypeinst_fact ef
inner join (Select sync_id, type_category_name, type_family_name, count((type_type_name)) as family_types From latest_categorygroup_dim Group By sync_id,type_category_name,type_family_name) as ft
on ef.sync_id = ft.sync_id
and ef.category = ft.type_category_name
and ef.family = ft.type_family_name
set ef.family_types = ifnull(ft.family_types,0);

update elemtypeinst_fact ef
inner join (Select sync_id, type_category_name, count((type_type_name)) as category_types From latest_categorygroup_dim Group by sync_id,type_category_name) as ct
on ef.sync_id = ct.sync_id
and ef.category = ct.type_category_name
set ef.category_types = ifnull(ct.category_types,0);

update elemtypeinst_fact ef
left join (select sync_id, type_category_name, count(inst_uniqueid) as categoryInstance from latest_instance_dim group by sync_id, type_category_name) as ci
on ef.sync_id = ci.sync_id
and ef.category = ci.type_category_name
set ef.categoryInstance = ifnull(ci.categoryInstance,0);

update elemtypeinst_fact ef
left join (select sync_id, type_category_name, type_family_name, count(inst_uniqueid) as familyInstance from latest_instance_dim group by sync_id, type_category_name, type_family_name) as fi
on ef.sync_id = fi.sync_id
and ef.category = fi.type_category_name
and ef.family = fi.type_family_name
set ef.familyInstance = ifnull(fi.familyInstance,0);

update elemtypeinst_fact ef
left join (select sync_id, type_category_name, type_family_name, type_type_name, count(inst_uniqueid) as typeInstance from latest_instance_dim group by sync_id, type_category_name, type_family_name, type_type_name) as ti
on ef.sync_id = ti.sync_id
and ef.category = ti.type_category_name
and ef.family = ti.type_family_name
and ef.type = ti.type_type_name
set ef.typeInstance = ifnull(ti.typeInstance,0);

SET SQL_SAFE_UPDATES = 1;

Create table ModelSummary_FACT as 
    Select pmsd.arup_projects_number as project_number,pmsd.mod_id,pmsd.sync_id, pmsd.mod_name as model_name, pmsd.sync_end,pmsd.sync_mb_start as filesize,
	ifnull(nc.categories,0) as categories, ifnull(nf.families,0) as families, ifnull(nt.types,0) as types, 
	ifnull(nv.views,0) as views, ifnull(ns.sheets,0) as sheets, ifnull(rl.RVTLinks,0) as RVTLinks, ifnull(cl.CADLinks,0) as CADLinks, 
	ifnull(nl.levels,0) as levels, ifnull(ni.instances,0) as instances, ifnull(nuc.categoryInstance,0) as categoryInstance, ifnull(nuf.familyInstance,0) as familyInstance, ifnull(nut.typeInstance,0) as typeInstance
        From Project_Mod_Sync_DIM pmsd
        left join (Select sync_id, count(*) as Views from View_DIM group by sync_id) as nv on pmsd.sync_id = nv.sync_id
        left join (Select sync_id, count(*) as Sheets from Sheet_DIM group by sync_id) as ns on pmsd.sync_id = ns.sync_id
        left join (Select sync_id, count(*) as RVTLinks from RVTLink_DIM group by sync_id) as rl on pmsd.sync_id = rl.sync_id
        left join (Select sync_id, count(*) as CADLinks from CADlink_DIM group by sync_id) as cl on pmsd.sync_id = cl.sync_id
        left join (Select sync_id, count(*) as Levels from Level_DIM group by sync_id) as nl on pmsd.sync_id = nl.sync_id
        left join (Select sync_id, count(*) as Instances from Instance_DIM group by sync_id) as ni on pmsd.sync_id = ni.sync_id
        left join (Select sync_id, count(distinct type_category_name) as categories From CategoryGroup_DIM Group By sync_id) as nc on pmsd.sync_id = nc.sync_id 
        left join (Select sync_id, count(distinct type_family_name) as families From CategoryGroup_DIM Group By sync_id) nf on pmsd.sync_id = nf.sync_id 
        left join (Select sync_id, count(*) as types From CategoryGroup_DIM Group By sync_id) as nt on pmsd.sync_id = nt.sync_id
        left join (Select sync_id, count(distinct type_category_name) as categoryInstance from Instance_DIM Group By sync_id) as nuc on pmsd.sync_id = nuc.sync_id
        left join (Select sync_id, count(distinct type_family_name) as familyInstance from Instance_DIM Group By sync_id) as nuf on pmsd.sync_id = nuf.sync_id
        left join (Select sync_id, count(distinct type_type_name) as typeInstance from Instance_DIM Group By sync_id) as nut on pmsd.sync_id = nut.sync_id;
     

Create or replace view ol_pl as 
    Select  msf.project_number,
            pd.short_title,
            pd.long_title,
            pd.project_type,
            msf.mod_id,
            msf.filesize as filesize,
            msf.categories as categories,
            msf.families as families,
            msf.types as types,
            msf.views as views,
            msf.sheets as sheets,
            msf.RVTLinks as RVTLinks,
            msf.CADLinks as CADLinks,
            msf.Levels as Levels,
            msf.instances as instances,
            msf.sync_end as latest_sync,
            pd.manager_name,
            pd.director_name
    From ModelSummary_FACT msf, Project_DIM pd
    Where msf.project_number = pd.project_number
    And msf.sync_end in (select max(sync_end) from ModelSummary_FACT group by mod_id order by mod_id, sync_end desc) group by mod_id;

Create or replace view ol_mod_sync as 
    Select msf.project_number,
       Count(distinct(msf.sync_id)) as syncs,
       Count(distinct(msf.mod_id)) as models
    From ModelSummary_FACT msf, Project_DIM pd, ModelSummary_FACT msf2 
    Where msf.project_number = pd.project_number 
    And msf2.sync_end in (select max(sync_end) from ModelSummary_FACT Group By project_number) Group By msf.project_number;

