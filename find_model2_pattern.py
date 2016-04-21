# -*- coding: utf-8 -*-
"""
Created on Fri Apr  1 21:03:47 2016

@author: Shen.Xu
"""

import pandas as pd
from pandas.util.testing import assert_frame_equal

yourpath='C:/Users/Shen.Xu/Desktop/SQL/Chiswick B7 Structural - CENTRAL_backup/'

pi = pd.read_csv(yourpath+'costpersquaremeter.csv')

pi.loc[pi['pi_text'].isin(['1058.728 m²'])]

pi.loc[pi['pi_text'].isin(['1058.728 m²'])]