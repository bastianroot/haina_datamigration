import pandas as pd
import numpy as np
import re
from tqdm import tqdm
pd.options.mode.chained_assignment = None  # default='warn'

#Funcion para quitar caracteres especiales y letras
def char_special(df, column):
    #Lista dada
    #Caracteres especiales a tener en cuenta
    char_special= ["iInternational", "inernacional", "INT", "intenacional", "intennacional", "INTER", "Intercontinental", "Internaciomal",
    "internacional", "INTERNACIONALES", "INTERNACIONTAL", "INTERNACIOTIONAL", "INTERNANTIONAL", "Internartional", "Internatinal",
    "INTERNATIONAL", "INTERNATIONATAL", "INTERNATIOTAL", "Internatonal", "interrnacional", "INTERTANATIONAL", "INTERTATIONAL",
    "INTE", "INTERATIONAL", "INTERNACIONAL", "INTERNATIONAL"]
    
    #Expresiones regulares
    special= re.compile("[\.\-\[\]\ ]")
    letter= re.compile("[a-zA-Z]")
    
    #Quitar simbolos
    df[column]= df[column].transform(lambda x: x.replace(special, ""))
    
    #Quitar letras que no coinciden con lista dada
    for i, r in enumerate(df[column]):
        if r not in char_special:
            try:
                df[column][i]= letter.sub("", r)
            except TypeError:
                pass
    return df[column]

def determineTypeofPerson(df):
    df["TIPO_PERSONA_DGII"]= ""
    pbar= tqdm(total = len(df["RNC"])+1)
    for i, r in enumerate(df["RNC"]):
        try:
            rncfromfile = str(int(r))
            lastDigit = rncfromfile[-2:]        
    
            if lastDigit == '11':
               df["TIPO_PERSONA_DGII"][i] = "Persona Natural"
            else:
               df["TIPO_PERSONA_DGII"][i] = "Persona Juridica"
        except:
            pass
        
        pbar.update(1)

def mergeDiiBP(dfDii, dfMp2, dfBPEpicor, dfBpVal, dfGiiProv, dfBpRastro):

    dfIndex = 0
    
    #Creacion de columnas
    newColumns = ["VENDORID", "NAME", "ADDR1", "ADDR2", "CITY", "COUNTRY", "REP","PHONE", "EXT", "TELEXFAX", "MP2CURRENCY", "EMAIL"]
    dfDii[newColumns] = np.nan 
    newColumnsEpicor = ["vendor_code", "address_name", "nat_cur_code"]
    dfDii[newColumnsEpicor] = np.nan
    newColumnsVendor = ["TIPO_DOCUMENTO", "TIPO_PERSONA", "CLASIFICACION_EMPRESARIAL", "RUBRO_PRINCIPAL", "TELEFONO_COMERCIAL", "DIRECCION", "MUNICIPIO", "PROVINCIA", "REGION", "MACROREGION" ]
    dfDii[newColumnsVendor] = np.nan
    newColumnsCheckBanck = ["addr6", "bank_account_num", "bank_name"]
    dfDii[newColumnsCheckBanck] = np.nan
    newColumnsRastro = ["Suplidor", "Email", "Email2"]
    dfDii[newColumnsRastro] = np.nan
    
    # dfBPResult = dfDii.iloc[:0].copy() 
    dfBPResult = pd.DataFrame()
    
    
    pbar= tqdm(total = len(dfDii["RNC"])+1)
    for i, rnc in enumerate(dfDii["RNC"]):
        b_rnc= True
        rowNumber = 0
        
        try:
            rncFromFile = int(rnc)
        except:
            b_rnc= False
            pass
        
        #Bool para controlar RNC NAN
        if b_rnc:
            #Consultas
            qResultMp2 = dfMp2.query(f"RNC == '{rncFromFile}'") #RNC Viene como String - dfDii tiene RNC como Int
            qResultMp2['RNC'] = qResultMp2['RNC'].astype('int64')
    
            qResultEpicor = dfBPEpicor.query(f"addr_sort2 == '{rncFromFile}'")
            qResultEpicor['addr_sort2'] = qResultEpicor['addr_sort2'].astype('int64')
    
            qResultVendor = dfGiiProv.query(f"NUMERO_DOCUMENTO == '{rncFromFile}'")          
            qResultVendor['NUMERO_DOCUMENTO'] = qResultVendor['NUMERO_DOCUMENTO'].astype('int64')        
            
            #Se hace proceso solo si hay resultado en MP2 o en Epicor
            if len(qResultMp2) > 0 or len(qResultEpicor) > 0:
            
                #Determinar numero de lineas a insertar
                if len(qResultMp2.index) > len(qResultEpicor.index):
                    rowNumber = len(qResultMp2.index)
                else:
                    rowNumber = len(qResultEpicor.index) 
        
                if len(qResultVendor.index) > rowNumber:
                    rowNumber = len(qResultVendor.index)
                
                if rowNumber > 0:
                    indexResult = len(dfBPResult.index)
                    #Agregar k líneas iguales de tabla base
                    for k in range(rowNumber):                
                        dfBPResult = dfBPResult.append(dfDii.loc[i],ignore_index=True)       
                #MP2         
                for indxMp2, rncMp2 in enumerate(qResultMp2.values):
                    
                    dfIndex = qResultMp2.index[indxMp2]
                    indexResult = indexResult + indxMp2
        
                    dfBPResult["VENDORID"][indexResult] = qResultMp2["VENDORID"][dfIndex]
                    dfBPResult["NAME"][indexResult] = qResultMp2["NAME"][dfIndex]
                    dfBPResult["ADDR1"][indexResult] = qResultMp2["ADDR1"][dfIndex]
                    dfBPResult["ADDR2"][indexResult] = qResultMp2["ADDR2"][dfIndex]
                    dfBPResult["CITY"][indexResult] = qResultMp2["CITY"][dfIndex]
                    dfBPResult["COUNTRY"][indexResult] = qResultMp2["COUNTRY"][dfIndex]
                    dfBPResult["REP"][indexResult] = qResultMp2["REP"][dfIndex]
                    dfBPResult["PHONE"][indexResult] = qResultMp2["PHONE"][dfIndex]
                    dfBPResult["EXT"][indexResult] = qResultMp2["EXT"][dfIndex]
                    dfBPResult["TELEXFAX"][indexResult] = qResultMp2["TELEXFAX"][dfIndex]
                    dfBPResult["MP2CURRENCY"][indexResult] = qResultMp2["MP2CURRENCY"][dfIndex]
                    dfBPResult["EMAIL"][indexResult] = qResultMp2["EMAIL"][dfIndex]
        
                    #VendorID obtenido de MP2
                    mp2_vendorid= qResultMp2["VENDORID"][dfIndex]
                    
                    try:
                        #Buscar vendorID en Rastro
                        qResultRastro = dfBpRastro.query(f"VendorID == '{mp2_vendorid}'")
                    except:
                        qResultRastro = dfBpRastro.query(f'VendorID == "{mp2_vendorid}"')
        
                    for indxViD, Mp2_ViD in enumerate(qResultRastro.values): 
                        
                        dfIndex = qResultRastro.index[indxViD]
                        indexResult = indexResult + indxViD
                      
                        dfBPResult["Suplidor"][indexResult] = qResultRastro["Suplidor"][dfIndex]
                        dfBPResult["Email"][indexResult] = qResultRastro["Email"][dfIndex]
                        dfBPResult["Email2"][indexResult] = qResultRastro["Email2"][dfIndex]  
        
                #EPICOR
                for indxEpicor, rncEpicor in enumerate(qResultEpicor.values):
                   
                    dfIndex = qResultEpicor.index[indxEpicor]
                    indexResult = indexResult + indxEpicor
        
                    dfBPResult["vendor_code"][indexResult] = qResultEpicor["vendor_code"][dfIndex]
                    dfBPResult["address_name"][indexResult] = qResultEpicor["address_name"][dfIndex]
                    dfBPResult["nat_cur_code"][indexResult] = qResultEpicor["nat_cur_code"][dfIndex]           
                                    
                    #Validación Bancos 
                    vendorCode= str(qResultEpicor["vendor_code"][dfIndex]).strip()    
                    qResultCheckBanck = dfBpVal.query(f"vendor_code == '{vendorCode}'")
                                
                    for indxCheckBanck, rncCheckBanck in enumerate(qResultCheckBanck.values):
        
                        dfIndex = qResultCheckBanck.index[indxCheckBanck]
                        indexResult = indexResult + indxCheckBanck
        
                        dfBPResult["addr6"][indexResult] = qResultCheckBanck["addr6"][dfIndex]
                        dfBPResult["bank_account_num"][indexResult] = qResultCheckBanck["bank_account_num"][dfIndex]
                        dfBPResult["bank_name"][indexResult] = qResultCheckBanck["bank_name"][dfIndex]
                        
                #Proveedores        
                for indxVendor, rncEpicor in enumerate(qResultVendor.values):
                    
                    dfIndex = qResultVendor.index[indxVendor]
                    indexResult = indexResult + indxVendor
                    
                    dfBPResult["TIPO_DOCUMENTO"][indexResult] = qResultVendor["TIPO_DOCUMENTO"][dfIndex]
                    #dfBPResult["TIPO_PERSONA"][indexResult] = qResultVendor["TIPO_PERSONA"][dfIndex]
                    dfBPResult["CLASIFICACION_EMPRESARIAL"][indexResult] = qResultVendor["CLASIFICACION_EMPRESARIAL"][dfIndex]
                    dfBPResult["RUBRO_PRINCIPAL"][indexResult] = qResultVendor["RUBRO_PRINCIPAL"][dfIndex]
                    dfBPResult["TELEFONO_COMERCIAL"][indexResult] = qResultVendor["TELEFONO_COMERCIAL"][dfIndex]
                    dfBPResult["DIRECCION"][indexResult] = qResultVendor["DIRECCION"][dfIndex]
                    dfBPResult["MUNICIPIO"][indexResult] = qResultVendor["MUNICIPIO"][dfIndex]
                    dfBPResult["PROVINCIA"][indexResult] = qResultVendor["PROVINCIA"][dfIndex]
                    dfBPResult["REGION"][indexResult] = qResultVendor["REGION"][dfIndex]
                    dfBPResult["MACROREGION"][indexResult] = qResultVendor["MACROREGION"][dfIndex]   
    
        pbar.update(1)
    return  dfBPResult

#Poner rutas de archivos a leer
bp_path= r"C:\Users\Wilson\Mi unidad\JDLSAS\Consultoria_Venezuela\Python BP\BP TABLA HOMOLOGACION.xlsx"
dgii_path= r"C:\Users\Wilson\Mi unidad\JDLSAS\Consultoria_Venezuela\Python BP\Exportacion dgii proveedor estado.xlsx"

print("Lectura de tablas pequeñas....")
#Lectura de hojas de archivo solicitadas
#BP TABLA HOMOLOGACION
df_bp_mp2= pd.read_excel(bp_path, "BP MP2")
df_bp_epicor= pd.read_excel(bp_path, "BP EPICOR")
df_bp_valida= pd.read_excel(bp_path, "BP EPICOR VALIDACION BANCARIA")
df_bp_rastro= pd.read_excel(bp_path, "BP RASTRO")

print("Lectura de tablas GRANDES..........")
#Exportacion dgii proveedor estado
#TODO activar
df_gii_rnc= pd.read_excel(dgii_path, "DGII_RNC")
#df_gii_rnc= pd.read_excel(r"C:\Users\Wilson\Mi unidad\JDLSAS\Consultoria_Venezuela\Python BP\dgii sola.xlsx", "DGII_RNC")
df_gii_prov= pd.read_excel(dgii_path, "proveedores")

print("Eliminando columnas innecesarias.......") 
#Eliminar columnas innecesarias de las tablas
#df_gii_rnc= df_gii_rnc.drop(['Column1', 'Fecha', 'Column11', 'Column12'], axis=1)

df_bp_mp2= df_bp_mp2.drop(['TERMS', 'VENDORBRANCHID', 'ADDR3', 'VENDOR_CODE'], axis=1)

df_bp_epicor= df_bp_epicor.drop(['addr6', 'addr_sort1', 'attention_phone', 'contact_name', 
                                 'contact_phone', 'vend_acct', 'tax_id_num', 'payment_code', 'country_code', 
                                 'attention_email', 'contact_email'], axis=1)

df_bp_valida= df_bp_valida.drop(['address_name'], axis=1)
#TODO activar
df_gii_prov= df_gii_prov.drop(['FECHA_REGISTRO', 'ESTADO_RPE', 'CLASIFICACION_RPE', 'ES_MIPYME', 'CERTIFICADO_MICM',
                              'CLASIFICACION_EMPRESARIAL_2', 'FECHA_ULTIMA_ACTUALIZACION', 'MOTIVO_INHABILITACION'], axis=1)

print("Eliminar caracteres especiales y letras.......")            
#Quitar caracteres especiales y letras de columnas pedidas
df_bp_mp2["RNC"]= char_special(df_bp_mp2, "RNC")
df_bp_epicor["addr_sort2"]= char_special(df_bp_epicor, "addr_sort2")
#TODO activar
df_gii_rnc["RNC"]= char_special(df_gii_rnc, "RNC")

#Determinar tipo de persona
print("Determinando tipo de persona......")
determineTypeofPerson(df_gii_rnc)

#Hacer cruce entre tablas por RNC
print("Iniciando cruce entre tablas.............................")
dfResult = mergeDiiBP(df_gii_rnc, df_bp_mp2, df_bp_epicor, df_bp_valida, df_gii_prov, df_bp_rastro)


print("FIN")
dfResult.to_excel(r"C:\Users\Wilson\Desktop\ReporteBP.xlsx",index=False)