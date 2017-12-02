# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 14:00:26 2017

@author: garance
"""
import pymysql
from private import *

connection = pymysql.connect(host=adress,
                             user=user,
                             password=mdp,
                             db="decaux",
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)