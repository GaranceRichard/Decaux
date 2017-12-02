# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 14:07:16 2017

@author: garance
"""

import pymysql
import private
mdp=private.mdp
adress=private.adress
user=private.user

connection = pymysql.connect(host=adress,
                             user=user,
                             password=mdp,
                             db="decaux")

with connection.cursor() as cursor:
    sqlnettoyage = "drop database decaux"
    cursor.execute(sqlnettoyage)
    
connection.close()