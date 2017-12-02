# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 13:50:07 2017

@author: garance
"""
import pymysql
from private import *

table=0
typical = pymysql.connect(host=adress,
                             user=user,
                             password=mdp,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

with typical.cursor() as cursor:
    sql = "show databases"
    cursor.execute(sql)
    result=cursor.fetchall()
    results=[d['Database'] for d in result]
    
    if "decaux" not in results:
        sql = "create database decaux CHARACTER SET utf32"
        cursor.execute(sql)

from connexion import connection
    
with connection.cursor() as cursor:   
    global result2    
    sql2 = "show tables"
    cursor.execute(sql2)
    results2=list(cursor.fetchall())
    result2=[d['Tables_in_decaux'] for d in results2]
    
    if "contracts" not in result2:
       sql3="create table contracts(id int auto_increment primary key, contract_name varchar(50),commercial_name varchar(20)) engine=innodb"
       cursor.execute(sql3)
       table += 1
    
    if "stations" not in result2:
        sql4="create table stations(id int auto_increment primary key, number int, name varchar(50), adress varchar(200),lat float,lng float,banking boolean, bonus boolean, id_contract int, CONSTRAINT FK_stationcontract foreign key (id_contract) references contracts(id)) engine=innodb"
        cursor.execute(sql4)
        table += 1
        
    if "positions" not in result2:
        sql5="create table positions(id int auto_increment primary key, status varchar(20), bike_stands int, available_bike_stands int, available_bikes int) engine=innodb"
        cursor.execute(sql5)
        table += 1
        
    if "position_station" not in result2:
        sql6="create table position_station(id bigint auto_increment primary key, id_station int,id_position int, last_update int, CONSTRAINT FK_idstation foreign key (id_station) references stations(id), CONSTRAINT FK_idpostions foreign key (id_position) references positions(id)) engine=innodb"
        cursor.execute(sql6)
        table += 1
        
    if "meteo" not in result2:
        sql1m="create table meteo(id bigint auto_increment primary key, cloudiness int, pressure int, temp int, weather varchar(30), windforce int, winddirection int, humidity int) engine=innodb"
        cursor.execute(sql1m)
        table += 1
    
    if "meteo_contracts" not in result2:
        sql2m="create table meteo_contracts(id bigint auto_increment primary key, id_contracts int, id_meteo bigint, lastupdate int, CONSTRAINT FK_idcontracts_meteoc foreign key (id_contracts) references contracts(id), CONSTRAINT FK_idmeteo_meteoc foreign key (id_meteo) references meteo(id)) engine=innodb"
        cursor.execute(sql2m)
        table += 1
        
    if "forecast3_contracts" not in result2:
        sql7m="create table forecast3_contracts(id bigint auto_increment primary key, id_contracts int, id_meteo bigint, datetime int, CONSTRAINT FK_idcontracts_forec3 foreign key (id_contracts) references contracts(id), CONSTRAINT FK_idmeteo_forec3 foreign key (id_meteo) references meteo(id)) engine=innodb"
        cursor.execute(sql7m)
        table += 1
    
    if "forecast6_contracts" not in result2:
        sql7m="create table forecast6_contracts(id bigint auto_increment primary key, id_contracts int, id_meteo bigint, datetime int, CONSTRAINT FK_idcontracts_forec6 foreign key (id_contracts) references contracts(id), CONSTRAINT FK_idmeteo_forec6 foreign key (id_meteo) references meteo(id)) engine=innodb"
        cursor.execute(sql7m)
        table += 1
    
    if "repeatt" not in result2:
        sqlrepeat="create table repeatt(id int auto_increment primary key, expression int) engine=innodb"
        cursor.execute(sqlrepeat)
        table += 1
        
    if "prediction_3h" not in result2:
        sqlprediction3="create table prediction_3h(id int auto_increment primary key,last_update int,id_station int,available_bikes int,available_bike_stands int, CONSTRAINT FK_station_prediction3 foreign key (id_station) references stations(id)) engine=innodb"
        cursor.execute(sqlprediction3)
        table += 1
        
    if "prediction_6h" not in result2:
        sqlprediction6="create table prediction_6h(id int auto_increment primary key,last_update int,id_station int,available_bikes int,available_bike_stands int, CONSTRAINT FK_station_prediction6 foreign key (id_station) references stations(id)) engine=innodb"
        cursor.execute(sqlprediction6)
        table += 1
        
connection.close()