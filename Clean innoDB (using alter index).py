# -*- coding: utf-8 -*-
"""
Created on Wed Apr 20 17:36:23 2016

@author: Shen.Xu
"""

create table IF NOT EXISTS params_inst_test like params_inst;

ALTER IGNORE TABLE params_inst_test
ADD UNIQUE INDEX idx_name (pi_type_id,
pi_name, 
pi_guid ,
pi_text,
pi_readonly , 
pi_numeric, 
pi_storage_type,
pi_group_name, 
pi_display_unit_type, 
pi_parameter_type);
                           
insert ignore into params_inst_test (select * from params_inst)