# -*- coding: utf-8 -*-
"""
Created on Wed Apr 20 17:19:10 2016

@author: Shen.Xu
"""

params_inst_all_new=pd.read_sql("""SELECT * FROM params_inst_new """ ,con)

params_inst_all_test=pd.read_sql("""SELECT * FROM params_inst_test""" ,con)

