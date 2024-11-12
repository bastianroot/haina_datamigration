import urllib
from statistics import median
from tqdm import tqdm
import pandas as pd
from sqlalchemy import create_engine

#Función para cálculo de promedios
def mediana(dataframe, column_prom, column_search):
   print("Inicio ciclo para Prom......")
   pbar= tqdm(total = len(dataframe["ITEMNUM"])+1)
   i= 0
   while i < len(dataframe["ITEMNUM"]):
       a_value= [] #Array para guardar valores       
       a_value.append(dataframe[column_search][i]) #Almacenar primer valor encontrado
       
       j= i
       #Mientras el valor siguiente sea igual
       if i != len(dataframe["ITEMNUM"]) - 1:
           while dataframe["ITEMNUM"][j] == dataframe["ITEMNUM"][j+1]:
               a_value.append(dataframe[column_search][j+1]) #Se agrega el valor al array
               j+= 1
               if j == len(dataframe["ITEMNUM"]) - 1:
                   break
       
       a_value= list(map(lambda x: abs(x), a_value)) #Convertir a valores positivos
       max_value= max(a_value) #Obtener el valor mayor
       a_value.sort() #Ordenar valores para calcular mediana
       mediana= int(median(a_value)) #Calculo de mediana
       
       while i <= j: #Agregar valores individuales o repetidos
           if column_prom == "Mediana Lead Time":
               dataframe[column_prom][i]= mediana
               dataframe["Maximo Lead Time"][i]= max_value  
           else:
               dataframe[column_prom][i]= mediana
           i+= 1 
           pbar.update(1)
   print("Fin de Prom......")
   if column_prom == "Mediana Lead Time":
       return dataframe[column_prom], dataframe["Maximo Lead Time"]
   else:
       return dataframe[column_prom]

def mediana_reabas(dataframe, column_prom, column_search):
    pbar= tqdm(total = len(dataframe["ITEMNUM"])+1)
    print("Inicio ciclo para mediana_reabas......")
    i= 0
    while i < len(dataframe["ITEMNUM"]):
        a_value= [] #Array para guardar valores
        a_value.append(dataframe[column_search][i]) #Almacenar primer valor encontrado 
        
        j= i
        #Mientras el valor siguiente sea igual
        if i != len(dataframe["ITEMNUM"]) - 1:
            while dataframe["ITEMNUM"][j] == dataframe["ITEMNUM"][j+1]:
                a_value.append(dataframe[column_search][j+1]) #Se agrega el valor al array
                j+= 1
                if j == len(dataframe["ITEMNUM"]) - 1:
                    break
        
        #Calcular diferencia fechas primera con segunda, segunda con tercera...
        k=0                
        if len(a_value) > 1:
            a_media= []
            while k < len(a_value):
                a_media.append(a_value[k] - a_value[k+1]) #Calcula diferencia entre fechas
                k+= 1
                if k == len(a_value) - 1:
                    break
                
            a_media= list(map(lambda x: abs(x.days), a_media)) #Convertir cada valor a dias
            a_media.sort() #Ordenar valores para calcular mediana
            mediana= int(median(a_media)) #Calculo de mediana
            
        else:
            mediana= 0 #Caso cuando no hay mas de 1 fecha              
        
        while i <= j: #Agregar valores individuales o repetidos
            dataframe[column_prom][i]= mediana        
            i+= 1 
            pbar.update(1)
    print("Inicio ciclo para Media Reabastecimiento......") 
    return dataframe[column_prom]    
    
ruta_tabla_homo= r"C:\Users\Wilson\Mi unidad\JDLSAS\Consultoria_Venezuela\Python Materiales\Tablas de homologación materiales.xlsx"

user= "Wvilla"
passw= "Haina#301501"
server= "192.168.1.164"
database= "MP2PROD"
#database= "master"
driver= "ODBC Driver 17 for SQL Server"

#Codificación rara Latin1
db_encode= {
    "charset": "cp-1252",
    "use_unicode": 1
    }

code= urllib.parse.urlencode(db_encode)

database_con= f"mssql+pyodbc://{user}:{passw}@{server}/{database}?driver={driver}&{code}"

engine = create_engine(database_con, echo=False, encoding="utf8")
conn= engine.connect()

#Tabla que tiene caracter raro
#tabla= "AnalisisCotDet"

#Consulta general a la tabla
df_cod_materiales= pd.read_sql_query('''
                                    SELECT DISTINCT 
                                    STOCK.ITEMNUM,
                                    INVY.[DESCRIPTION],
                                    INVY.[UOM],
                                    INVY.[OEMMFG],
                                    STOCK.WAREHOUSEID,
                                    STOCK.[LOCATION],
                                    STOCK.[QTYONHAND],
                                    ISNULL(WAREHOUSEINFO.[MINSTOCKLEVEL],0) AS MINSTOCKLEVEL ,
                                    ISNULL(WAREHOUSEINFO.[MAXSTOCKLEVEL],0)AS MAXSTOCKLEVEL,
                                    WAREHOUSEINFO.[ABCCLASS],
                                    WAREHOUSEINFO.[STOCKITEM]
                                    FROM [MP2PROD].[dbo].[STOCK]
                                    LEFT JOIN [MP2PROD].[dbo].[INVY]
                                    ON [MP2PROD].[dbo].[STOCK].[ITEMNUM]=[MP2PROD].[dbo].[INVY].[ITEMNUM]
                                    LEFT JOIN [MP2PROD].[dbo].[WAREHOUSEINFO]
                                    ON [MP2PROD].[dbo].[STOCK].[ITEMNUM]=[MP2PROD].[dbo].[WAREHOUSEINFO].[ITEMNUM] AND [MP2PROD].[dbo].[STOCK].[WAREHOUSEID]=[MP2PROD].[dbo].[WAREHOUSEINFO].[WAREHOUSEID]
                                    WHERE STOCK.WAREHOUSEID IN ('ALM SULTANA','ALM SULTANA US','COMBUSTIBLE','ALM PROYECTOS','ALM QUISQUEYA 2','ALM Q2 US',
                                    'ALM PIPELINEO&M','ALM HAINA 1','ALM BARAHONA','ALM PEDERNALES','ALM LOS COCOS','ALM LARIMAR','ALM PARQUE GIRA',
                                    'ALM P GIRA US','ALM PALENQUE','ALM PALENQUE US') AND [DESCRIPTION] NOT IN ('CODIGO INHABIITADO','CODIGO INHABILIADO',
                                    'CODIGO INHABILITADO','CODIGO INHABIUTADO','CODIGOINHABILITADO','CODIGO INHABILITADO, REPETIDO X MEC-29-COM-PCDN200','CODIGO INHABILITADO UTILIZAR EL MEC-29-COM-SK46342') AND QTYONHAND>0 order by ITEMNUM
                                  ''', conn)
                                  
df_max_min= pd.read_sql_query('''
                            SELECT 
                            PORECEIV.[PONUM],
                            [SEQNUM],
                            [ITEMNUM],
                            [DATERECEIVED],
                            POHEADER.[ORDERDATE],
                            [QTYRECEIVED],
                            [UNITCOST],
                            [RECEIVEWAREHOUSE]
                            FROM [MP2PROD].[dbo].[PORECEIV]
                            LEFT JOIN [MP2PROD].[dbo].[POHEADER]
                            ON [MP2PROD].[dbo].[PORECEIV].[PONUM]=[MP2PROD].[dbo].[POHEADER].[PONUM]
                            WHERE [RECEIVEWAREHOUSE] IN ('ALM SULTANA','ALM SULTANA US','COMBUSTIBLE','ALM PROYECTOS','ALM QUISQUEYA 2','ALM Q2 US',
                            'ALM PIPELINEO&M','ALM HAINA 1','ALM BARAHONA','ALM PEDERNALES','ALM LOS COCOS','ALM LARIMAR','ALM PARQUE GIRA',
                            'ALM P GIRA US','ALM PALENQUE','ALM PALENQUE US') AND [DATERECEIVED] BETWEEN'2017-01-01' AND '2022-08-31' order by ITEMNUM
                            ''', conn)
                            
df_consumo_materiales= pd.read_sql_query('''
                                    SELECT 
                                    ISSUEP.ITEMNUM,
                                    ISSUEP.EQNUM,
                                    ISSUEP.QTY,
                                    ISSUEP.WAREHOUSEID,
                                    INVY.[DESCRIPTION],
                                    YEAR(ISSUEP.DATEOUT) AS YEAR
                                    FROM [MP2PROD].[dbo].[ISSUEP]
                                    LEFT JOIN [MP2PROD].[dbo].[INVY]
                                    ON [MP2PROD].[dbo].[ISSUEP].[ITEMNUM]=[MP2PROD].[dbo].[INVY].[ITEMNUM]
                                    WHERE ISSUEP.WAREHOUSEID IN ('ALM SULTANA','ALM SULTANA US','COMBUSTIBLE','ALM PROYECTOS','ALM QUISQUEYA 2','ALM Q2 US',
                                    'ALM PIPELINEO&M','ALM HAINA 1','ALM BARAHONA','ALM PEDERNALES','ALM LOS COCOS','ALM LARIMAR','ALM PARQUE GIRA',
                                    'ALM P GIRA US','ALM PALENQUE','ALM PALENQUE US') AND [DESCRIPTION] NOT IN ('CODIGO INHABIITADO','CODIGO INHABILIADO',
                                    'CODIGO INHABILITADO','CODIGO INHABIUTADO','CODIGOINHABILITADO','CODIGO INHABILITADO, REPETIDO X MEC-29-COM-PCDN200','CODIGO INHABILITADO UTILIZAR EL MEC-29-COM-SK46342') order by ITEMNUM
                                  ''', conn)

#Conversión a tabla pandas
#df_cod_materiales = pd.DataFrame(cod_materiales)
#df_max_min = pd.DataFrame(max_min)
#df_consumo_materiales = pd.DataFrame(consumo_materiales)


print("Calculando Lead_Time......")
#Agregar columna con la diferencia entre ORDERDATE y DATERECEIVED / Media reabastecimiento?
df_max_min["Lead_Time"]= df_max_min["DATERECEIVED"] - df_max_min["ORDERDATE"]
df_max_min["Lead_Time"]= df_max_min["Lead_Time"].transform(lambda x: x.days)

print("Creando columnas extra en Codigo Materiales......")
#Creación columnas nuevas para cod materiales
df_cod_materiales["Diccionario"]=""
df_cod_materiales["Mediana Lead Time"]= ""
df_cod_materiales["Maximo Lead Time"]= ""
df_cod_materiales["Mediana Consumo"]= ""
df_cod_materiales["Mediana Reabas"]= ""
df_cod_materiales["Stock de seguridad"]= ""
df_cod_materiales["Punto reorden"]= ""
df_cod_materiales["Minimo"]= ""
df_cod_materiales["Maximo"]= ""
df_cod_materiales["Part Number"]= ""
df_cod_materiales["New Description"]= ""

print("Creando columnas extra en Max Min")
#Creación columnas nuevas para max_min
df_max_min["Mediana Lead Time"]= ""
df_max_min["Mediana Reabas"]= ""
df_max_min["Maximo Lead Time"]= ""

print("Creando columnas extra en Consumo de Materiales......")
#Creación columnas nuevas para consumo materiales
df_consumo_materiales["Mediana Consumo"]= ""

print("")
print("Calculo Mediana Lead Time....")
#Cálculo de promedios Mediana Lead Time
df_max_min["Mediana Lead Time"], df_max_min["Maximo Lead Time"]= mediana(df_max_min, "Mediana Lead Time", "Lead_Time")
print("Calculo Mediana Consumo Materiales....")
#Cálculo de promedios Mediana de Consumo Materiales
df_consumo_materiales["Mediana Consumo"]= mediana(df_consumo_materiales, "Mediana Consumo", "QTY")
print("Calculo Mediana Reabastecimiento....")
#Calculo Mediana de Reabastecimiento
df_max_min["Mediana Reabas"]= mediana_reabas(df_max_min, "Mediana Reabas", "DATERECEIVED")


print("")
print("Inicio recorrido ITEMNUM....")
#Calculo de Stock de Seguridad - Punto Reorden - Minimo - Maximo
pbar= tqdm(total = len(df_cod_materiales["ITEMNUM"])+1)
for i, item in enumerate(df_cod_materiales["ITEMNUM"]):
    
    try:
        #Buscar item en tabla Max Min
        media= df_max_min.query(f"ITEMNUM == '{item}'")
    except:
        media= df_max_min.query(f'ITEMNUM == "{item}"')
    
    try:
        #Buscar item en tabla Consumo Materiales
        media_consumo= df_consumo_materiales.query(f"ITEMNUM == '{item}'")
    except:
        media_consumo= df_consumo_materiales.query(f'ITEMNUM == "{item}"') 
    
    #Validar si ambas consultas traen datos antes de reemplazar sus valores
    aux= False
    if len(media) > 0 and len(media_consumo) > 0:
        aux= True
    
    if len(media) > 0 :
        #Asignar valores de Media Reabastecimiento
        for m in media["Mediana Reabas"]:
            media_reabas= m
            df_cod_materiales["Mediana Reabas"][i] = m #Agregar valor a tabla cod materiales
            break
        
        #Asignar valores de Media Lead Time
        for m in media["Mediana Lead Time"]:
            media_lead= m
            df_cod_materiales["Mediana Lead Time"][i] = m #Agregar valor a tabla cod materiales
            break
        
        #Asignar valores de Maximo Lead Time
        for m in media["Maximo Lead Time"]:
            max_lead_time= m
            df_cod_materiales["Maximo Lead Time"][i] = m #Agregar valor a tabla cod materiales
            break
    
    if len(media_consumo) > 0:
        for m in media_consumo["Mediana Consumo"]:
            media_consumo= m
            df_cod_materiales["Mediana Consumo"][i] = m
            break
        
    if aux:    
        #Agregar Stock de seguridad        
        stock_seguridad= (max_lead_time - media_lead) * media_consumo
        df_cod_materiales["Stock de seguridad"][i]= stock_seguridad #Agregar valor a tabla cod materiales
        
        #Agregar Punto Reorden
        df_cod_materiales["Punto reorden"][i]= stock_seguridad + media_lead * media_consumo
        
        #Agregar Minimo
        minimo= int(stock_seguridad + media_reabas * (media_consumo/365))
        df_cod_materiales["Minimo"][i]= minimo
        
        #Agregar Maximo
        df_cod_materiales["Maximo"][i]= int(minimo + media_reabas * (media_consumo/365))
    
    pbar.update(1)

print("Se extrajeron Stock de Seguridad - Punto Reorden - Minimo - Maximo...")
print("")

print("Lectura de hoja Excel Almacen y centros...")
#Leer hoja Almacen y centros para conversiones del WAREHOUSEID
df_almacen_centros= pd.read_excel(ruta_tabla_homo, "Almacen y centros", header=2)
print("Extrayendo valores a homologar...")
#Conversión de valor a homologar y clave org como Dict
val_clave_org= dict(zip(df_almacen_centros["VALOR A HOMOLOGAR"], df_almacen_centros['Clave Org\xa0']))
#Conversión de valor a homologar y denominacion como Dict
val_denomina= dict(zip(df_almacen_centros["VALOR A HOMOLOGAR"], df_almacen_centros['Denominación\xa0']))
print("Convirtiendo Clave Org, Denominacion, Almacen y Alm. Denominacion......")
#Conversión de palabras a clave org
df_cod_materiales["Clave Org"]= df_cod_materiales["WAREHOUSEID"][df_cod_materiales["WAREHOUSEID"].isin(val_clave_org)].replace(val_clave_org)
#Conversión de palabras a clave Denominacion
df_cod_materiales["Denominacion"]= df_cod_materiales["WAREHOUSEID"][df_cod_materiales["WAREHOUSEID"].isin(val_denomina)].replace(val_denomina)
#Numero estandar segun tabla
df_cod_materiales["Almacen"]= 1000
#Valor estandar segun tabla
df_cod_materiales["Alm. Denominacion"]= "Almacén General"


print("")
print("Lectura de hoja Excel dic_palabra_clave...")
#Lectura de hoja dic_palabra_clave
df_dic_palabras_clave= pd.read_excel(ruta_tabla_homo, "dic_palabra_clave")
print("Extrayendo valores a homologar...")
#Conversión de palabras recurrentes y diccionario a un dict
dictionary= dict(zip(df_dic_palabras_clave["Palabras recurrentes"], df_dic_palabras_clave["Diccionario"]))
print("Convirtiendo Diccionario......")
#Separar primer palabra de columna #DESCRIPTION
df_palabras= df_cod_materiales["DESCRIPTION"].transform(lambda x: x.split(" "))
#Agregar columna con palabras convertidas
#df_cod_materiales["Diccionario"]= df_palabras[df_palabras.isin(dictionary)].replace(dictionary)
pbar= tqdm(total = len(df_palabras)+1)
for i, descrip in enumerate(df_palabras):
    aux= False
    for desc in enumerate(descrip):
        if desc[1] in dictionary:
            aux= True
            df_cod_materiales["Diccionario"][i]= desc[1].replace(desc[1], dictionary[desc[1]])
            break
    pbar.update(1)        


print("")
print("Lectura de hoja Excel pcvsga y Grupo_articulosGC...")
#Lectura de hoja pcvsga
df_pcvsga= pd.read_excel(ruta_tabla_homo, "pcvsga")
df_grupo_articulosGC= pd.read_excel(ruta_tabla_homo, "Grupos_articulosGC")
print("Extrayendo valores a homologar......")
#Conversión de pcvsga a codigo de articulos a un dict
pcvsga_codigo= dict(zip(df_pcvsga["Diccionario"], df_pcvsga["Grupo de artículos"]))
#Conversión de pcvsga a grupo de articulos a un dict
pcvsga_denominacion= dict(zip(df_pcvsga["Diccionario"], df_pcvsga["Denominacion "]))
#Conversión de Grupo_articulosGC a grupo de articulos a un dict
gru_art_clave_grp= dict(zip(df_grupo_articulosGC["Código"], df_grupo_articulosGC["Clave Grp\xa0"]))
#Conversión de Grupo_articulosGC a grupo de articulos a un dict
gru_art_clave_denomi= dict(zip(df_grupo_articulosGC["Código"], df_grupo_articulosGC["Denominación Grupo de Compra\xa0"]))
print("Convirtiendo Codigo y Grupo de Articulo......")
#Agregar columna con codigo de articulos
df_cod_materiales["Codigo"]= df_cod_materiales["Diccionario"][df_cod_materiales["Diccionario"].isin(pcvsga_codigo)].replace(pcvsga_codigo)
#Agregar columna con grupo de articulos
df_cod_materiales["Grupo Articulo"]= df_cod_materiales["Diccionario"][df_cod_materiales["Diccionario"].isin(pcvsga_denominacion)].replace(pcvsga_denominacion)
#Agregar columna con clave de grupo
df_cod_materiales["Clave Grp"]= df_cod_materiales["Codigo"][df_cod_materiales["Codigo"].isin(gru_art_clave_grp)].replace(gru_art_clave_grp)
#Agregar columna con Denominacion Grupo de Compra
df_cod_materiales["Denominacion Grupo de Compra"]= df_cod_materiales["Codigo"][df_cod_materiales["Codigo"].isin(gru_art_clave_denomi)].replace(gru_art_clave_denomi)


print("")
print("Lectura de hoja Excel UOM...")
#Lectura de hoja UOM
df_UOM= pd.read_excel(ruta_tabla_homo, "UOM")
print("Extrayendo valores a homologar......")
#Conversión de pcvsga a codigo de articulos a un dict
UOM_SAP= dict(zip(df_UOM["UNIT"], df_UOM["UOM SAP"]))
print("Convirtiendo Codigo y Grupo de Articulo......")
#Agregar columna con grupo de articulos
df_cod_materiales["UOM SAP"]= df_cod_materiales["UOM"][df_cod_materiales["UOM"].isin(UOM_SAP)].replace(UOM_SAP)


print("")
print("Lectura de hoja Excel Clave_id_PN...")
#Lectura de hoja Clave_id_PN
df_clave_id_pn= pd.read_excel(ruta_tabla_homo, "Clave_id_PN")
#Conversión de pcvsga a codigo de articulos a un dict
identificador= df_clave_id_pn["Identificación part numbers"]

print("Iniciando extraccion de Part Number")
for i, item in enumerate(df_cod_materiales["DESCRIPTION"]):
    l_match= list(filter(lambda x: x in item, identificador))
    
    if len(l_match) > 0:
        idx= item.index(l_match[0])
        id_len= len(l_match[0])
        part_number= item[idx + id_len:]
        new_desc= item[idx:]
        
        df_cod_materiales["Part Number"][i]= part_number
        df_cod_materiales["New Description"][i]= item.replace(new_desc, "")

print("")
print("Lectura de hoja Excel Ubicaciones...")
#Lectura de hoja Clave_id_PN
df_location= pd.read_excel(ruta_tabla_homo, "Ubicaciones")
print("Extrayendo valores a homologar......")
#Conversión de pcvsga a codigo de articulos a un dict
new_location= dict(zip(df_location["LOCATION"], df_location["New_Location"]))
print("Convirtiendo Codigo y Grupo de Articulo......")
#Agregar columna con grupo de articulos
df_cod_materiales["New_Location"]= df_cod_materiales["LOCATION"][df_cod_materiales["LOCATION"].isin(new_location)].replace(new_location)


#df_cod_materiales["New_Location"]= df_cod_materiales["LOCATION"][df_cod_materiales["UOM"].isin(new_location)].replace(new_location)



##############################
#Ver verdadero nombre columnas
#df_grupo_articulosGC.columns[4]
##############################
print("")
print("Exportando archivo...")
df_cod_materiales.to_excel(r"C:\Users\Wilson\Desktop\Reporte Materiales.xlsx")

















