# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 13:20:47 2017

@author: Garance
"""

import  pandas as pd
import time
import pymysql.cursors
import requests
import private

#Initialisation des variables
apiKey = private.apiKey
apim = private.apim
mdp=private.mdp
adress=private.adress
user=private.user
repeat=turn=total=0
pause=10
urlcontracts = 'https://api.jcdecaux.com/vls/v1/contracts?apiKey='+ str(apiKey)

#Initialisation de la DB
tps0=time.clock()
import test_table
table=test_table.table   
tps1 = time.clock()
print("temps création des tables : \t %.2f \t tables crées : \t %d" % (tps1-tps0,table))   


while repeat < 10:
    turn += 1
    print("\nturn : {}".format(turn))
    tps2 = time.clock()
    
    #Extraction des datas
        #Définition des contrats - variable : contracts
    dfcontracts = pd.read_json(urlcontracts)
    dictcontracts = pd.Series.to_dict(dfcontracts['name'])
    contracts = list(dictcontracts.values())  
    dfcontracts=dfcontracts[["name","commercial_name"]]

        #Définition des positions et harshing - variable : df(entier) dfc(contrats)
    urlwork = "https://api.jcdecaux.com/vls/v1/stations?contract="+contracts[0]+"&apiKey="+str(apiKey)
    df=(pd.read_json(urlwork))
    
    for x in contracts[1:]:
        urlwork = "https://api.jcdecaux.com/vls/v1/stations?contract="+str(x)+"&apiKey="+str(apiKey)
        df1 = pd.read_json(urlwork)
        df = df.append(df1)
        
    df['last_update']=df['last_update']/1000
    df['last_update']=df['last_update'].astype(int)
    
    df=df.sort_values(["contract_name","number"])
    df=pd.concat([df.drop(['position']),df['position'].apply(pd.Series)],axis=1)
    df[['lat','lng']].astype('float32')
    
    df=df.merge(dfcontracts,left_on="contract_name",right_on="name")
    df['name']=df['name_x']
    
    df=df[['contract_name','commercial_name','number','name','address','lat', 'lng','banking','bonus','last_update','status','bike_stands','available_bike_stands','available_bikes']]
    
    df.index = range(1,len(df) + 1)
    del (df1,dfcontracts,x)
    dfc=df[['contract_name','commercial_name']].drop_duplicates()
    
    taille = len(df)
    
#initialisation en contract[0]    
    
    def generate_actual_dfweather(city):
        if str(city) == 'Goteborg':
            city='Goeteborg'
        if str(city) == 'Cergy-Pontoise':
            city= 'Cergy'
        if str(city) == 'Toyama':
            city="Toyama-shi"
        url = "http://api.openweathermap.org/data/2.5/weather?q="+str(city)+"&APPID="+ str(apim)
        content=requests.get(url)
        data=content.json()
        name=data["name"]
        cloudiness=data["clouds"]['all']
        pressure=int(data["main"]["pressure"])
        temp=int(data['main']['temp']-273.15)
        weather=data['weather'][0]["description"]
        windforce=int(data['wind']['speed']*3.6)
        if "deg" in data["wind"].keys():
            winddirection=int(data["wind"]["deg"])
        else:
            winddirection=0
        humidity=int(data["main"]["humidity"])
        datetime=data["dt"]
        dictweather = {"City":[name],"Cloudiness":[cloudiness],"Pressure":[pressure],"Temp":[temp],"Weather":[weather],"Windforce":[windforce],"Datetime":[datetime],"Winddirection":[winddirection],"Humidity":[humidity]}
        dfweather = pd.DataFrame(dictweather)
        return dfweather
    
    def generate_forecast_dfweather(city,hour):        
        if str(city) == 'Goteborg':
            city='Goeteborg'
        if str(city) == 'Cergy-Pontoise':
            city= 'Cergy'
        if str(city) == 'Toyama':
            city="Toyama-shi"        
        url = "http://api.openweathermap.org/data/2.5/forecast?q="+str(contracts[0])+"&APPID="+ str(apim)
        content=requests.get(url)
        data=content.json()
        name=data["city"]["name"]
        forecastposition=int(hour/3-1)
        heure=data["list"][forecastposition]["dt"]
        temp=int(data["list"][forecastposition]['main']['temp']-273.15)
        pressure=int(data["list"][forecastposition]["main"]["pressure"])
        weather=data["list"][forecastposition]['weather'][0]["description"]
        if "deg" in data["list"][forecastposition]["wind"].keys():
            winddirection=int(data["list"][forecastposition]["wind"]["deg"])
        else:
            winddirection=0
        windforce=int(data["list"][forecastposition]['wind']['speed']*3.6)
        cloudiness=data["list"][forecastposition]["clouds"]['all']
        humidity = data["list"][forecastposition]["main"]["humidity"]
        dictforecast = {"City":[name],"Cloudiness":[cloudiness],"Pressure":[pressure],"Temp":[temp],"Weather":[weather],"Windforce":[windforce],"Datetime":[heure],"Winddirection":[winddirection],"Humidity":[humidity]}
        dfforecast = pd.DataFrame(dictforecast)
        return dfforecast
        
        
    dfweather=generate_actual_dfweather(contracts[0])
    for city in contracts[1:]:
        df1 = generate_actual_dfweather(city)
        dfweather = dfweather.append(df1)
    dfweather.City=contracts
    
    dfforeca3=generate_forecast_dfweather(contracts[0],3)
    for city in contracts[1:]:
        df1 = generate_forecast_dfweather(city,3)
        dfforeca3 = dfforeca3.append(df1)
    dfforeca3.City=contracts

    dfforeca6=generate_forecast_dfweather(contracts[0],6)
    for city in contracts[1:]:
        df1 = generate_forecast_dfweather(city,6)
        dfforeca6 = dfforeca6.append(df1)
    dfforeca6.City=contracts    

    
    
    tps3=time.clock()
    print("temps extraction/wrangling : \t %.2f \t taille du df : \t %d" % (tps3-tps2,taille))
    
    dfc=dfc.values.tolist()
    contrat = 0
    
    connection = pymysql.connect(host=adress,
                                 user=user,
                                 password=mdp,
                                 db="decaux",
                                 charset='utf8mb4')
    
    with connection.cursor() as cursor:   
        sql7 = "SELECT * FROM contracts"
        cursor.execute(sql7)
        results3=list(cursor.fetchall())
        result3=[i[1] for i in results3]
    
        for rows in dfc:
            if rows[0] not in result3:
                sql8="insert into contracts(contract_name,commercial_name) values (%s, %s)"
                data = (rows[0],rows[1])
                cursor.execute(sql8,data)
                connection.commit()
                contrat += 1
    
    tps4=time.clock()
    print("temps création contrats : \t %.2f \t contrats crées : \t %d" % (tps4-tps3,contrat))
    
    df['name']=df['name'].str.replace('\u0152', 'OE')
    df['address']=df['address'].str.replace('\u0152', 'OE')
    df['name']=df['name'].str.replace('\u2013', '-')
    df['address']=df['address'].str.replace('\u2013', '-')
    df['name']=df['name'].str.replace('\u2019', ' ')
    df['address']=df['address'].str.replace('\u2019', ' ')
    
    dfstations=df[['contract_name','number', 'name', 'address', 'lat','lng', 'banking', 'bonus']].values.tolist()
    station = 0
    
    with connection.cursor() as cursor:
        sql9 = "SELECT * from stations"
        cursor.execute(sql9)
        results4=list(cursor.fetchall())
        result4=[i[2] for i in results4]
    
        for rows in dfstations:
            if (rows[2] not in result4):
                sql10="insert into stations(number,name,adress,lat,lng,banking,bonus,id_contract) values (%s, %s, %s, %s, %s, %s, %s, (SELECT id FROM contracts WHERE contract_name = %s))"
                data = (rows[1],rows[2],rows[3],rows[4],rows[5],rows[6],rows[7],rows[0])
                cursor.execute(sql10,data)
                connection.commit()
                station += 1
    
    tps5=time.clock()
    print("temps création stations : \t %.2f \t stations crées : \t %d" % (tps5-tps4,station))
    
    dfpositions=df[["status","bike_stands","available_bike_stands","available_bikes"]]
    dfpositions=dfpositions.drop_duplicates().values.tolist()
    position = 0
    
    with connection.cursor() as cursor:
        sql11 = "SELECT * from positions"
        cursor.execute(sql11)
        results5=list(cursor.fetchall())
        result5=[list(elem) for elem in results5]
        result5=[sublist[1:] for sublist in result5]
    
        for rows in dfpositions:
            if (rows not in result5):
                sql12="insert into positions(status,bike_stands,available_bike_stands,available_bikes) values (%s, %s, %s, %s)"
                data = (rows[0],rows[1],rows[2],rows[3])
                cursor.execute(sql12,data)
                connection.commit()
                result5.append(rows)
                position += 1
    
    tps6=time.clock()
    print("temps création positions : \t %.2f \t positions crées : \t %d" % (tps6-tps5,position))
    
    dfweatherpos=dfweather[["Cloudiness","Pressure","Temp","Weather","Windforce","Winddirection","Humidity"]].values.tolist()
    ddfforeca3=dfforeca3[["Cloudiness","Pressure","Temp","Weather","Windforce","Winddirection","Humidity"]].values.tolist()
    ddfforeca6=dfforeca6[["Cloudiness","Pressure","Temp","Weather","Windforce","Winddirection","Humidity"]].values.tolist()
    posweather = 0
    
    with connection.cursor() as cursor:
        sql3m="SELECT * from meteo"
        cursor.execute(sql3m)
        results3m=list(cursor.fetchall())
        result3m=[list(elem) for elem in results3m]
        result3m=[x[1:] for x in result3m]
        
        for rows in dfweatherpos:
            if rows not in result3m:
                sql4m="insert into meteo(cloudiness,pressure,temp,weather,windforce,winddirection,humidity) values(%s, %s, %s, %s, %s, %s, %s)"
                data= (rows[0],rows[1],rows[2],rows[3],rows[4],rows[5],rows[6])
                cursor.execute(sql4m,data)
                connection.commit()
                result3m.append(rows)
                posweather += 1
        
        for rows in ddfforeca3:
            if rows not in result3m:
                sql4mb="insert into meteo(cloudiness,pressure,temp,weather,windforce,winddirection,humidity) values(%s, %s, %s, %s, %s, %s, %s)"
                data= (rows[0],rows[1],rows[2],rows[3],rows[4],rows[5],rows[6])
                cursor.execute(sql4mb,data)
                connection.commit()
                result3m.append(rows)
                posweather += 1
        
        for rows in ddfforeca6:
            if rows not in result3m:
                sql4mt="insert into meteo(cloudiness,pressure,temp,weather,windforce,winddirection,humidity) values(%s, %s, %s, %s, %s, %s, %s)"
                data= (rows[0],rows[1],rows[2],rows[3],rows[4],rows[5],rows[6])
                cursor.execute(sql4mt,data)
                connection.commit()
                result3m.append(rows)
                posweather += 1


                
    tps1m=time.clock()
    print("temps creation meteos : \t %.2f \t météos crées : \t %d" % (tps1m-tps6,posweather))
    
    dfpositionsstations=df[["name","address","status","bike_stands","available_bike_stands","available_bikes","last_update"]].values.tolist()
    posta = 0
    
    with connection.cursor() as cursor:
        sql13="SELECT max(last_update) FROM position_station"
        cursor.execute(sql13)
        results6=list(cursor.fetchall())
        result6=results6[0][0]
    
        for rows in dfpositionsstations:
            if (result6 is None) or (rows[6] > (result6+1)) :
                sql14 = "INSERT INTO position_station(id_station,id_position,last_update) VALUES ((SELECT id FROM stations WHERE name = %s AND adress = %s),(SELECT id FROM positions WHERE status = %s AND bike_stands = %s AND available_bike_stands = %s AND available_bikes = %s),%s)"
                data = (rows[0],rows[1],rows[2],rows[3],rows[4],rows[5],rows[6])
                cursor.execute(sql14,data)
                connection.commit()
                posta += 1
    
    tps7=time.clock()
    print("temps positions_stations : \t %.2f \t positions maj : \t %d" % (tps7-tps1m,posta))


    dfweatherpos2=dfweather.values.tolist()
    pos2weather = 0
        
    with connection.cursor() as cursor:
        sql5m="select max(lastupdate) from meteo_contracts"
        cursor.execute(sql5m)
        results5m=list(cursor.fetchall())
        result5m=results5m[0][0]
    
        for rows in dfweatherpos2:
            if (result5m is None) or (rows[2] > (result5m+1)):
                sql6m = "insert into meteo_contracts(id_contracts,id_meteo,lastupdate) values ((select id from contracts where contract_name = %s),(select id from meteo where cloudiness = %s and pressure = %s and temp = %s and weather = %s and windforce = %s and winddirection = %s and humidity = %s),%s)"
                data=(rows[0],rows[1],rows[4],rows[5],rows[6],rows[8],rows[7],rows[3],rows[2])
                cursor.execute(sql6m,data)
                connection.commit()
                pos2weather += 1

    tps2m=time.clock()
    print("temps positions meteo : \t %.2f \t meteos maj : \t\t %d" % (tps2m-tps7,pos2weather))

    dfforeca3=dfforeca3.values.tolist()
    for3weather = 0
        
    with connection.cursor() as cursor:
        sql8m="select max(datetime) from forecast3_contracts"
        cursor.execute(sql8m)
        results8m=list(cursor.fetchall())
        result8m=results8m[0][0]
    
        for rows in dfforeca3:
            if (result8m is None) or (rows[2] > (result8m+1)):
                sql9m = "insert into forecast3_contracts(id_contracts,id_meteo,datetime) values ((select id from contracts where contract_name = %s),(select id from meteo where cloudiness = %s and pressure = %s and temp = %s and weather = %s and windforce = %s and winddirection = %s and humidity = %s),%s)"
                data=(rows[0],rows[1],rows[4],rows[5],rows[6],rows[8],rows[7],rows[3],rows[2])
                cursor.execute(sql9m,data)
                connection.commit()
                for3weather += 1

    tps3m=time.clock()
    print("temps forecast 3h : \t\t %.2f \t forecast maj : \t %d" % (tps3m-tps2m,for3weather))
    
    
    dfforeca6=dfforeca6.values.tolist()
    for6weather = 0
        
    with connection.cursor() as cursor:
        sql10m="select max(datetime) from forecast6_contracts"
        cursor.execute(sql10m)
        results10m=list(cursor.fetchall())
        result10m=results10m[0][0]
    
        for rows in dfforeca6:
            if (result10m is None) or (rows[2] > (result10m+1)):
                sql11m = "insert into forecast6_contracts(id_contracts,id_meteo,datetime) values ((select id from contracts where contract_name = %s),(select id from meteo where cloudiness = %s and pressure = %s and temp = %s and weather = %s and windforce = %s and winddirection = %s and humidity = %s),%s)"
                data=(rows[0],rows[1],rows[4],rows[5],rows[6],rows[8],rows[7],rows[3],rows[2])
                cursor.execute(sql11m,data)
                connection.commit()
                for6weather += 1

    tps4m=time.clock()
    print("temps forecast 6h : \t\t %.2f \t forecast maj : \t %d" % (tps4m-tps3m,for6weather))
    
    print("temps total : \t\t\t %.2f \t imports et ajouts : \t %d" % (tps4m-tps2,taille+table+contrat+station+position+posta+posweather+pos2weather+for3weather+for6weather))
    
    connection.close()
    
    total += table+contrat+station+position+posta+posweather+pos2weather+for3weather+for6weather
    if ((tps4m-tps2)<pause):
        print("pause : %d sec | tps global : \t %d \t intégrations bdd : \t %d" % ((pause-(tps4m-tps2)),tps4m-tps0,total))
        time.sleep(int((pause-(tps4m-tps2))))
        
    repeat += 1