import urllib
import jdcal
import pandas as pd
from sqlalchemy import create_engine
from xmlparser import xml_class
from juliana_convert import normal_date
from dao import dbConnectionEngine


#Lee plantilla
file= r"C:\Users\Wilson\Mi unidad\JDLSAS\Consultoria_Venezuela\Plantillas\FI-1707\Source data for Exchange rate.xml"
df_file= xml_class.import_xml(file, 2, 6, False)


#Obtener nombre de columnas
new_column_name= []
for c in df_file.iloc[0]:
    name= c.split("\n")[0]
    new_column_name.append(name)

#Nombre de columnas auxiliar
aux_column_name= [c for c in new_column_name]
#Agregar diferenciador para columnas con igual nombre  
aux_column_name[5]= aux_column_name[5] + "1"
aux_column_name[6]= aux_column_name[6] + "1"

#Poner nuevos nombres
df_file.columns= aux_column_name
df_file= df_file.drop([0],axis=0)

#Conexión a Base de Datos
user= "Slondono"
passw= "Haina#502730"
host= "192.168.1.164"
database= "HAINA_IFRSControl"
driver= "ODBC Driver 17 for SQL Server"

dbConnection = dbConnectionEngine()
dbConnection.getDBConnection("mysql", host, "", driver, database, user, passw, False)

tabla= "mccurtdt"
#tabla= "artrxage"
sql_query = pd.read_sql_query(f'select * from {tabla}', dbConnection.connection)

df_mccurtdt= pd.DataFrame(sql_query)

df_file['From currency*']= df_mccurtdt['from_currency'].replace("RD", "DOP")
df_file['Exchange rate type*'] = "M"
df_file['To-currency*']= df_mccurtdt['to_currency'].replace("RD", "DOP") #Reemplazo de tipos de monedas
df_file['Date from Which Entry Is Valid*']= [normal_date(c_date) for c_date in df_mccurtdt['convert_date']] #Conversión de fechas
df_file['Indirect Quoted Exchange Rate']= df_mccurtdt['buy_rate']
df_file['Ratio for the "from" currency units']= 1 #Siempre lleva 1 si Indirect Quoted Exchange Rate está lleno
df_file['Ratio for the "to" currency units']= 1 #Siempre lleva 1 si Indirect Quoted Exchange Rate está lleno

#Eliminar filas que no tienen fecha
df_file["date"]= df_file['Date from Which Entry Is Valid*']
t= df_file.query('date != "" ')
df_file= t
df_file= df_file.drop(['date'], axis=1)

#Hacer filtro de acuerdo a las llaves
df_file= df_file.drop_duplicates(subset=['From currency*', 'Exchange rate type*', 'To-currency*', 'Date from Which Entry Is Valid*'])

#Devolver nombres originales de la tabla
df_file.columns= new_column_name

#Generar archivo de Excel
df_file.to_excel(r"C:\Users\Wilson\Desktop\Plantilla Tipo de Cambio V2.xlsx", index= False)






