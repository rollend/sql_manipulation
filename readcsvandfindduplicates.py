# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 10:01:28 2016

@author: Shen.Xu
"""

import pandas as pd
from pandas.util.testing import assert_frame_equal

yourpath='C:/Users/Shen.Xu/Desktop/SQL/Duplicated data validation'

pi = pd.read_csv(yourpath+'/pi_id.csv')

duplicate_id = pd.read_csv(yourpath+'/duplicate.csv')

pi_duplicated=pi[pi.duplicated(['pi_name','pi_group_name','pi_parameter_type','pi_type_id'])]

pi_clean = pd.read_csv(yourpath+'/pi_id(cleaned).csv')

pi__clean_duplicated=pi[pi_clean.duplicated(duplicates_parameters)]

duplicates_parameters=['pi_type_id', 'pi_name', 'pi_numeric','pi_storage_type','pi_parameter_type','pi_group_name','pi_text','pi_readonly','pi_display_unit_type']
#PI.pi_type_id, PI.pi_name, PI.pi_numeric,PI.pi_storage_type,PI.pi_parameter_type,PI.pi_group_name,PI.pi_text
pi_duplicated=pi[pi.duplicated(duplicates_parameters)]

pi__clean_duplicated=pi[pi_clean.duplicated(duplicates_parameters)]

#PI.pi_type_id, PI.pi_name, PI.pi_group_name,PI.pi_text
['pi_name','pi_group_name','pi_parameter_type','pi_type_id']

['pi_type_id', 'pi_name', 'pi_guid', 'pi_group_name','pi_text']


pi2=pi.groupby(['pi_name','pi_group_name','pi_parameter_type','pi_type_id']).filter(lambda pi:pi.shape[0] > 1)

pi4=pi.groupby(duplicates_parameters).filter(lambda pi:pi.shape[0] > 1)

pi5=pi_clean.groupby(duplicates_parameters).filter(lambda pi:pi.shape[0] > 1)

pi_duplicated2=pi[pi.duplicated(['pi_name', 'pi_group_name','pi_parameter_type','pi_type_id','pi_text','pi_numeric'])]

ewq=pi_duplicated2[~pi_duplicated2.pi_id.isin(pi3.pi_id)]