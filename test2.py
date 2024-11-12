import pandas as pd
from tqdm import tqdm
from juliana_convert import normal_date
from dao import dbConnectionEngine
from time import sleep
from datetime import datetime

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
                
                #Prueba cambio suma != 0
                #if round(suma, 5) == 0 or round(suma, 5) < 0:
                if round(suma, 5) == 0:
                    index_list= list(range(index_aux, index))
                    for index_list_pos in index_list:
                        delete_row_index.append(index_list_pos)
                    
                    # df_art_apt= df_art_apt.drop(range(index_aux, index, 1), axis=0) #Eliminar filas que no estarán en el reporte
                    # df_art_apt.reset_index()
                    # total_len= len(df_art_apt['apply_to_num'])
                
                #Prueba cambio suma != 0
                #elif round(suma, 10) > 0:
                elif round(suma, 10) > 0 or round(suma, 10) < 0:
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

def filtrar_fecha(df, fecha_corte):
    df_aux = pd.DataFrame()
    
    print("Antes de filtrar fechas: ", len(df))
    #Filtrar por fecha los registros
    df['date_applied']= pd.to_datetime(df['date_applied'], format='%d.%m.%Y')
    df_aux = df.loc[ df['date_applied'] <= datetime.strptime( fecha_corte, '%d.%m.%Y') ]
    df_aux['date_applied']= df_aux['date_applied'].dt.strftime('%d.%m.%Y')
    print("Despues de filtrar fechas: ",len(df_aux))
    
    return df_aux

def doc_reference(df):   
    
    #Nuevas columnas
    df["Sociedad"]= "DO01"
    
    if tabla == "artrxage":
        df["Número de documento de referencia"]= df["cust_po_num"]        
    elif tabla == "aptrxage":
        df["Número de documento de referencia"]= ""
        
    df["Número de partida individual"]= ""
    
    if tabla == "artrxage":
        df["Deudor"]= "" 
        df["Cuenta contrapartida"] = "9999999002"
    elif tabla == "aptrxage":
        df["Acreedor"]= ""
        df["Cuenta contrapartida"] = "9999999003"    
    
    df["Clase documento"]= "" 
    df["Fecha documento"]= df["date_doc"]    
    df["Texto cabecera"]= ""
    df["Texto posicion"]= "CARGA INICIAL"
    df["Moneda transaccion"]= df["nat_cur_code"]    
    df["Cantidad transaccion"]= df["amount"]    
    df["Moneda sociedad"]= "DOP"
    df["Cantidad sociedad"]= ""    
    df["Moneda de grupo"]= "USD"   
    df["Cantidad grupo"]= ""
    df["Cantidad dias"]= ""
    df["Condicion pago"]= ""
    df["Fecha linea base"]= df["date_due"]
    
    dict_cant_dias= {0: "Z001", 3: "Z002", 7: "Z003", 10: "Z004", 15: "Z005", 20: "Z006", 25: "Z007",
                     30: "Z008", 40: "Z009", 60: "Z010", 90: "Z011", 120: "Z012"}
    
    
    #Convertir date_doc y date_due a fechas
    df['date_doc']= pd.to_datetime(df['date_doc'], format='%d.%m.%Y')
    df['date_due']= pd.to_datetime(df['date_due'], format='%d.%m.%Y')
    #Ordenar por apply_to_num y date_doc
    df.sort_values(by=["apply_to_num", "date_doc"], inplace=True, ascending = [True, True])
    
    #Index reset
    df= df.reset_index(drop=True)
    
    index= 0
    delete_row_index= [] #Lista para eliminar filas
    total_len= len(df['apply_to_num']) #Longitud total a recorrer
    pbar= tqdm(total = total_len + 1)
    
    while index < total_len: 
        num_partida= 10
        row = df.iloc[index] #Current row index 
        apply= row['apply_to_num'] #Current apply_to_num       
        trx= row['trx_ctrl_num'] #Current trx
        
        index_aux= index #Variable auxiliar para control del index        
        b_apply= True #Validar siguiente trx
        b_referencia= False
        b_trx_type= False
        
        while b_apply:
                     
            if apply == trx and not b_referencia: #Buscar número de referencia
                referencia= row['doc_ctrl_num']
                refer_index= index
                b_referencia= True
            
            if row['trx_type'] == 4113:
                b_trx_type= True
            
            index+= 1   #Aumenta en 1 el index - Nuevo valor del Index
            
            if index > (total_len-1):
                apply_next= "Fin del ciclo"
            else:
                row = df.iloc[index] #Fila siguiente
                try:                    
                    apply_next= row['apply_to_num']  #Valor del siguiente apply
                    trx= row['trx_ctrl_num'] #Valor del siguiente trx
                except Exception as inst:
                    apply_next= ""
            
            if apply != apply_next:
                b_apply= False
                index_list= list(range(index_aux, index)) #Rango de index a modificar
                
                #Bool para validar si existe trx_type == 4113
                if b_trx_type:
                    k= 0 #Contador                    
                    
                    while(k < len(index_list)):
                        if index_list[k] != refer_index: 
                            delete_row_index.append(index_list[k]) #Almacenar en lista de filas a eliminar
                            index_list.pop(k) #Eliminar elemento de lista index
                        else:
                            k+= 1
                
                
                #Iterar sobre elementos restantes de index_list
                for i in index_list:
                    try:
                                                   
                        if tabla == "aptrxage": 
                            #Columna Numero de documento de referencia
                            df.loc[i, "Número de documento de referencia"]= referencia     
                        
                        #Clase de documento y Cantidad de dias y Condicion de Pago
                        if df.loc[i, "trx_type"] == 2032:
                            df.loc[i, "Clase documento"]= "ZC"
                        elif df.loc[i, "trx_type"] == 2111:
                            df.loc[i, "Clase documento"]= "DG"                            
                        elif df.loc[i, "trx_type"] == 4111:
                            df.loc[i, "Clase documento"]= "KZ"
                        elif df.loc[i, "trx_type"] == 4161:
                            df.loc[i, "Clase documento"]= "KG"
                        elif df.loc[i, "trx_type"] == 4091 or df.loc[i, "trx_type"] == 2031:
                            
                            if tabla == "artrxage":
                                df.loc[i, "Clase documento"]= "DR"
                            elif tabla == "aptrxage":
                                df.loc[i, "Clase documento"]= "KR"
                            
                            #Cantidad de dias 
                            df.loc[i, "Cantidad dias"]= abs((df.loc[i, "date_doc"] - df.loc[i, "date_due"]).days)                                
                            #Condicion de Pago
                            if df.loc[i, "Cantidad dias"] in dict_cant_dias:
                                df.loc[i, "Condicion pago"]= dict_cant_dias[df.loc[i, "Cantidad dias"]] 
                        
                        #Cantidad grupo  
                        if df.loc[i, "nat_cur_code"] == "USD":
                            df.loc[i, "Cantidad grupo"]= df.loc[i, "amount"]  
                        
                        elif df.loc[i, "nat_cur_code"] == "RD" or df.loc[i, "nat_cur_code"] == "EUR":                                
                            if df.loc[i, "rate_home"] < 0 and df.loc[i, "amount"] > 0 and tabla == "aptrxage":
                                df.loc[i, "Cantidad grupo"]= (df.loc[i, "amount"] / df.loc[i, "rate_home"]) * -1
                            
                            elif df.loc[i, "rate_home"] < 0:
                                df.loc[i, "Cantidad grupo"]= (df.loc[i, "amount"] / df.loc[i, "rate_home"]) * -1                               
                            
                            elif df.loc[i, "rate_home"] > 0:
                                df.loc[i, "Cantidad grupo"]= df.loc[i, "amount"] * df.loc[i, "rate_home"]
                        
                        
                        #Cantidad de Sociedad
                        if df.loc[i, "nat_cur_code"] == "RD":
                            df.loc[i, "Cantidad sociedad"]= df.loc[i, "amount"]
                        elif df.loc[i, "nat_cur_code"] == "USD" or df.loc[i, "nat_cur_code"] == "EUR":
                            df.loc[i, "Cantidad sociedad"]= df.loc[i, "amount"] * df.loc[i, "rate_oper"]
                        
                        df.loc[i, "Número de partida individual"]= num_partida
                        num_partida+= 10
                        
                    except:
                        pass
            
            pbar.update(1)
    
    try:
        df.drop(df.index[delete_row_index], inplace=True)
    except KeyError as error:
        print(error)
    
    #Devolver formato a fecha
    df['date_doc']= df['date_doc'].dt.strftime('%d.%m.%Y')
    df['date_due']= df['date_due'].dt.strftime('%d.%m.%Y')
    
    return df


#Conexión a Base de Datos
user= "Slondono"
passw= "Haina#502730"
host= "192.168.1.164"
database= "HAINA_IFRS"
driver= "ODBC Driver 17 for SQL Server"

dbConnection = dbConnectionEngine()
dbConnection.getDBConnection("mssql", host, "", driver, database, user, passw, False)

print(".....Extrayendo datos de la DB")
#####################
#Escoger la tabla para la cual se desea hacer la extracción
#
#tabla= "artrxage"
tabla= "aptrxage"
fecha_corte= '28.02.2023'
reporte= "4"

ruta_doc_interno= r"C:\temp\HAINA\Documento interno.xlsx"

#Para Generar Archivo
if tabla == "artrxage":
    customer= 'customer_code' #Nombre columna tabla resumen
    name= 'Deudores' #Nombre archivo según tabla
    ruta_BP= r"C:\temp\HAINA\BP CLIENTES.xlsx"
elif tabla == "aptrxage":
    customer= 'vendor_code' #Nombre columna tabla resumen
    name= 'Acreedores' #Nombre archivo según tabla
    ruta_BP= r"C:\temp\HAINA\BP PROVEEDORES.xlsx"

#####################
#Conexion a la Base de Datos
df_art_apt = pd.read_sql_query(f'select * from {tabla} order by apply_to_num', dbConnection.connection)

#Transformaciones
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


#Genera solo reporte 1
if reporte == "1":
    #Generación de archivo con conversión de fechas
    df_art_apt.to_excel(r"C:\temp\HAINA\Partidas abiertas de " + name + " - Reporte 1.xlsx", index= False) 

elif reporte == "2":
    
    #Extraccion de datos
    print("......")
    print("......Inicio de extracción de datos")
    df_art_apt, a_resume= extraction(df_art_apt)
    print("......Finalizada extracción de datos\n")
    print("......")

elif reporte == "3":
    
    #Filtro de fechas
    print("......Inicio filtro para Fecha de Corte")
    df_art_apt_fl = filtrar_fecha(df_art_apt.copy(), fecha_corte)
    
    #Extraccion de datos
    print("......")
    print("......Inicio de extracción de datos")
    df_art_apt, a_resume= extraction(df_art_apt)
    print("......Finalizada extracción de datos\n")
    print("......")    
    
 
elif reporte == "4":    
    
    
    #Filtro de fechas en base a reporte 3
    print("......Inicio filtro para Fecha de Corte")
    df_art_apt_fl = filtrar_fecha(df_art_apt.copy(), fecha_corte)
    
    #df_art_apt_fl = df_art_apt[ df_art_apt['date_applied'] <= datetime.strptime( '31.01.2023', '%d.%m.%Y').date() ]

    #Extraccion de datos
    print("......")
    print("......Inicio de extracción de datos")
    df_art_apt_fl, a_resume= extraction(df_art_apt_fl)
    print("......Finalizada extracción de datos\n")
    print("......")

    ##############Reporte 4#################
    print("......Iniciando modificaciones para Reporte #4\n")
    print("......")
    df_art_apt_fl = doc_reference(df_art_apt_fl.copy())
    
    print("")
    print("Lectura de hoja Excel BP...")
    #Lectura de hoja BP
    df_BP= pd.read_excel(ruta_BP, skiprows=1, header=None)
    df_BP[1]= df_BP[1].astype("str")
    df_BP[1]= df_BP[1].transform(lambda x: x.zfill(10))
    print("Extrayendo valores a homologar......")
    #Conversión a un dict
    BP = dict(zip(df_BP[0], df_BP[1]))
    print("Convirtiendo Valores......")
    #Reemplazar nuevos valores
    if tabla == "artrxage":
        df_art_apt_fl["Deudor"]= df_art_apt_fl["customer_code"][df_art_apt_fl["customer_code"].isin(BP)].replace(BP)
    
    elif tabla == "aptrxage":
        df_art_apt_fl["Acreedor"]= df_art_apt_fl["vendor_code"][df_art_apt_fl["vendor_code"].isin(BP)].replace(BP)
    
    
    print("")
    print("Lectura de hoja Excel Documento Interno...")
    #Lectura de hoja BP
    df_doc_interno= pd.read_excel(ruta_doc_interno)
    print("Extrayendo valores a homologar......")
    #Conversión a un dict
    doc_interno = dict(zip(df_doc_interno["Voucher No."], df_doc_interno["Internal Comment"]))
    print("Convirtiendo Voucher e Internal Comment......")
    #Agregar valores segun apply_to_num
    df_art_apt_fl["Texto cabecera"]= df_art_apt_fl["apply_to_num"][df_art_apt_fl["apply_to_num"].isin(doc_interno)].replace(doc_interno)
    
    
    #Conversion de Moneda de Transaccion
    df_art_apt_fl['Moneda transaccion']= df_art_apt_fl['Moneda transaccion'].replace("RD", "DOP")



if reporte != "1":    
    
    print("......Creación de Dataframe con Resumen")
    #Creación de hoja auxiliar con el resumen    
    df_aux= pd.DataFrame(a_resume, columns=['apply_to_num', customer,'amount'])
    
    print("......Inicia creación de Excel")
    #Creación de archivo de Excel con dos hojas
    writer = pd.ExcelWriter(r"C:\temp\HAINA\ " + name + " Reporte #" + reporte + " V3 suma!=0.xlsx", engine='xlsxwriter')
    df_art_apt_fl.to_excel(writer, sheet_name="Sheet1",index= False)
    df_aux.to_excel(writer, sheet_name="Sheet2", index= False)
    writer.close()
    
    print("......FIN......")
