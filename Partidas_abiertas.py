import urllib
import jdcal
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine
from xmlparser import xml_class
from juliana_convert import normal_date
from time import sleep

#Conversión fecha
#normal_date(735598)


user= "Wvilla"
passw= "Haina#301501"
host= "192.168.1.164"
database= "HAINA_IFRS"
#database= "master"
driver= "ODBC Driver 17 for SQL Server"
#driver= "SQL Server"

db_encode= {
    "charset": "cp-1252",
    "use_unicode": 1
    }

code= urllib.parse.urlencode(db_encode)


database_con= f"mssql+pyodbc://{user}:{passw}@{host}/{database}?driver={driver}&{code}"

#Metodo Sebas
#connection_ = f"mssql+pyodbc://{user}:{urllib.parse.quote_plus(password)}@{host}/{dbname}{driver}"

engine = create_engine(database_con, echo=False)
conn= engine.connect()

print(".....Extrayendo datos de la DB")
#####################
#Escoger la tabla para la cual se desea hacer la extracción
#
#tabla= "artrxage"
tabla= "aptrxage"
#####################
df_art_apt = pd.read_sql_query(f'select * from {tabla} order by apply_to_num', conn)

#Ver verdadero nombre columnas
#df_artrxage.columns[15]

sleep(1)
print("............Convirtiendo fechas")
df_art_apt['date_doc']= df_art_apt['date_doc'].transform(lambda x: normal_date(x))
df_art_apt['date_due']= df_art_apt['date_due'].transform(lambda x: normal_date(x))
df_art_apt['date_applied']= df_art_apt['date_applied'].transform(lambda x: normal_date(x))
df_art_apt['date_aging']= df_art_apt['date_aging'].transform(lambda x: normal_date(x))
df_art_apt['date_paid']= df_art_apt['date_paid'].transform(lambda x: normal_date(x))

#Generar archivo para Reporte #1
if tabla == "artrxage":
    customer= 'customer_code' #Nombre columna tabla resumen
    name= 'Deudores' #Nombre archivo según tabla
elif tabla == "aptrxage":
    customer= 'vendor_code' #Nombre columna tabla resumen
    name= 'Acreedores' #Nombre archivo según tabla
    
df_art_apt.to_excel(r"C:\Users\Wilson\Desktop\Partidas abiertas de " + name + " - Reporte 1.xlsx", index= False)

sleep(1)
print("......Inicio de extracción de datos")

index= 0
total_len= len(df_art_apt['apply_to_num']) #Longitud total a recorrer
pbar= tqdm(total = total_len + 1)
a_resume= []

while index < total_len:
    atn= df_art_apt['apply_to_num'][index] #Current atn
    atn_next= df_art_apt['apply_to_num'][index + 1] 
    suma= 0 #Suma de cada elemento en "amount"
    index_aux= index #Variable auxiliar para control del index
        
    #a_idx= [] #Arreglo para controlar filas a eliminar
    
    b_atn= True #Validar siguiente atn
    while b_atn:
        #a_idx.append(index) #Agregar número index a eliminar si es el caso
        suma+= df_art_apt['amount'][index] #Realiza suma de campo "amount"           
        index+= 1   #Aumenta en 1 el index - Nuevo valor del Index
        
        if index > (total_len-1):
            atn_next= "Fin del ciclo"
        else:
            atn_next= df_art_apt['apply_to_num'][index]  #Valor del siguiente atn
        
        if atn != atn_next:
            b_atn= False
            if round(suma, 5) == 0 or round(suma, 5) < 0:
                df_art_apt= df_art_apt.drop(range(index_aux, index, 1), axis=0) #Eliminar filas que no estarán en el reporte
            
            elif round(suma, 10) > 0:
                if tabla == "artrxage":
                    customer= df_art_apt['customer_code'][index - 1]
                elif tabla == "aptrxage":
                    customer= df_art_apt['vendor_code'][index - 1]
                    
                a_resume.append([atn, customer, suma])
        
        pbar.update(1)        

print("......Finalizada extracción de datos\n")
print("......")
print("......Creación de Dataframe con Resumen")        
#Creación de hoja auxiliar con el resumen

    
df_aux= pd.DataFrame(a_resume, columns=['apply_to_num', customer,'amount'])

print("......Inicia creación de Excel")

writer = pd.ExcelWriter(r"C:\Users\Wilson\Desktop\Partidas Abiertas " + name +" Reporte #2.xlsx", engine='xlsxwriter')
df_art_apt.to_excel(writer, sheet_name="Sheet1",index= False)
df_aux.to_excel(writer, sheet_name="Sheet2", index= False)
writer.close()

print("......FIN......")




