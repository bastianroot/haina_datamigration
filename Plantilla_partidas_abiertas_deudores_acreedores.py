import pandas as pd
from tqdm import tqdm
from juliana_convert import normal_date
from dao import dbConnectionEngine
from time import sleep
from datetime import date,datetime, timedelta

#Conversión fecha
#normal_date(735598,1900)

def extraction(df_art_apt):
    index= 0
    total_len= len(df_art_apt['apply_to_num']) #Longitud total a recorrer
    pbar= tqdm(total = total_len + 1)
    delete_row_index = []
    a_resume= []
    
    while index < total_len:
        #+SLS 02032023 - Reporte #3{
        row = df_art_apt.iloc[index]   
        # -SLS Se cambia para obtener mejor performance{
        # atn= df_art_apt['apply_to_num'][index] #Current atn        
        # atn_next= df_art_apt['apply_to_num'][index + 1]
        #} 
        atn= row['apply_to_num'] #Current atn        

        suma= 0 #Suma de cada elemento en "amount"
        index_aux= index #Variable auxiliar para control del index
            
        #a_idx= [] #Arreglo para controlar filas a eliminar
        
        b_atn= True #Validar siguiente atn
        while b_atn:
            #a_idx.append(index) #Agregar número index a eliminar si es el caso
            
            # suma+= df_art_apt['amount'][index] #Realiza suma de campo "amount"           
            suma+= row['amount'] #Realiza suma de campo "amount"  
            index+= 1   #Aumenta en 1 el index - Nuevo valor del Index
            
            if index > (total_len-1):
                atn_next= "Fin del ciclo"
            else:
                row = df_art_apt.iloc[index]
                try:
                    # atn_next= df_art_apt['apply_to_num'][index]  #Valor del siguiente atn
                    atn_next= row['apply_to_num']  #Valor del siguiente atn
                except Exception as inst:
                    atn_next= ""
            
            if atn != atn_next:
                b_atn= False
                if round(suma, 5) == 0 or round(suma, 5) < 0:
                    index_list= list(range(index_aux, index))
                    for index_list_pos in index_list:
                        delete_row_index.append(index_list_pos)
                    
                    # df_art_apt= df_art_apt.drop(range(index_aux, index, 1), axis=0) #Eliminar filas que no estarán en el reporte
                    # df_art_apt.reset_index()
                    # total_len= len(df_art_apt['apply_to_num'])
                
                elif round(suma, 10) > 0:
                    previous_row = df_art_apt.iloc[ index - 1 ]
                    if tabla == "artrxage":
                        # customer= df_art_apt['customer_code'][index - 1]
                        customer= previous_row['customer_code']
                    elif tabla == "aptrxage":
                        # customer= df_art_apt['vendor_code'][index - 1]
                        customer= previous_row['vendor_code']
                        
                    a_resume.append([atn, customer, suma])
            
            pbar.update(1)
    try:
        df_art_apt.drop(df_art_apt.index[delete_row_index], inplace=True)
    except KeyError as error:
        print(error)

    return df_art_apt, a_resume

#Conexión a Base de Datos
user= "Slondono"
passw= "Haina#502730"
host= "192.168.1.164"
database= "HAINA_IFRS"
driver= "ODBC Driver 17 for SQL Server"

dbConnection = dbConnectionEngine()
dbConnection.getDBConnection("mysql", host, "", driver, database, user, passw, False)

print(".....Extrayendo datos de la DB")
#####################
#Escoger la tabla para la cual se desea hacer la extracción
#
#tabla= "artrxage"
tabla= "aptrxage"
reporte= "#3"
#####################
df_art_apt = pd.read_sql_query(f'select * from {tabla} order by apply_to_num', dbConnection.connection)

#Ver verdadero nombre columnas
#df_artrxage.columns[15]

sleep(1)
print("............Convirtiendo fechas")
print("............Transformando date_doc")
df_art_apt['date_doc']= df_art_apt['date_doc'].transform(lambda x: normal_date(x, 1900))
print("............Transformando date_due")
df_art_apt['date_due']= df_art_apt['date_due'].transform(lambda x: normal_date(x, 1900))
print("............Transformando date_applied")
df_art_apt['date_applied']= df_art_apt['date_applied'].transform(lambda x: normal_date(x, 1900))
print("............Transformando date_aging")
df_art_apt['date_aging']= df_art_apt['date_aging'].transform(lambda x: normal_date(x, 1900))
print("............Transformando date_paid")
df_art_apt['date_paid']= df_art_apt['date_paid'].transform(lambda x: normal_date(x, 1900))

# +SLS: Filtrar fecha:
if reporte == "#3":
    df_art_apt_fl = pd.DataFrame()

    print("Antes de filtrar fechas: ", len(df_art_apt))
    #Filtrar por fecha los registros
    df_art_apt_fl = df_art_apt[ df_art_apt['date_applied'] <= datetime.strptime( '31.01.2023', '%d.%m.%Y').date() ]
    print("Despues de filtrar fechas: ",len(df_art_apt_fl))
#Generar archivo para Reporte #1
if tabla == "artrxage":
    customer= 'customer_code' #Nombre columna tabla resumen
    name= 'Deudores' #Nombre archivo según tabla
elif tabla == "aptrxage":
    customer= 'vendor_code' #Nombre columna tabla resumen
    name= 'Acreedores' #Nombre archivo según tabla

###############
#Habilitar para generar Reporte #1
# df_art_apt.to_excel(r"C:\temp\HAINA\Partidas abiertas de " + name + " - Reporte 1.xlsx", index= False)
###############

sleep(1)
print("......Inicio de extracción de datos")

df_art_apt_fl, a_resume= extraction(df_art_apt_fl)    

print("......Finalizada extracción de datos\n")
print("......")

print("......Creación de Dataframe con Resumen")        

#Creación de hoja auxiliar con el resumen    
df_aux= pd.DataFrame(a_resume, columns=['apply_to_num', customer,'amount'])

print("......Inicia creación de Excel")

#Creación de archivo de Excel con dos hojas
writer = pd.ExcelWriter(r"C:\temp\HAINA\Partidas Abiertas " + name +" Reporte #3.xlsx", engine='xlsxwriter')
df_art_apt_fl.to_excel(writer, sheet_name="Sheet1",index= False)
df_aux.to_excel(writer, sheet_name="Sheet2", index= False)
writer.close()

print("......FIN......")




