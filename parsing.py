from sqlalchemy import String, Numeric   
import xml.etree.ElementTree as ET
from dao import dbConnectionEngine
import pandas as pd
import json

class parsingData:

    dbTypeEquivalence = {"Text": String, "Date": String, "Time": String, "Number": Numeric }    
    dbLengthTypeEquivalence = {"Date": 8, "Time": 6, "Text": 500 }
    def __init__(self) -> None:
        pass

    def loadXMLFile(self,sourceFile, WorksheetNumber=1):
        #file= r"C:\Users\Wilson\Desktop\test2.xml"
        file_json= r"C:\Users\Wilson\Desktop\JSONtest.json"
        file = sourceFile
        #Namespace para Excel
        ns = {"doc": "urn:schemas-microsoft-com:office:spreadsheet"}

        tree= ET.parse(file)
        root= tree.getroot()

        ws= root.findall('.//doc:Worksheet', ns) #Busca todos los Worksheet
        row= ws[WorksheetNumber].findall('.//doc:Row', ns) #Busca solamente la segunda hoja

        data= []

        for k, r in enumerate(row):
            if k > 2:                
                cell= r.findall('doc:Cell', ns)
                colum= []
                i=1 #Contador de columnas
                for c in cell:
                    #Ver si viene Index                    
                    index= c.attrib
                    
                    #Validar existencia Index
                    if "{" + f"{ns['doc']}" + "}" + "Index" not in index:
                        #Si no existe, agrega el dato
                        value= c.find('doc:Data', ns)
                        try:
                            colum.append(value.text)
                            print(value.text)
                            print("not found")
                        except AttributeError:
                            colum.append("")
                            print("not found")                     
                    else:
                        #Si existe, agrega cantidad de espacios                        
                        value_index= int(index["{" + f"{ns['doc']}" + "}" + "Index"])
                        if i < value_index:
                            for j in range(value_index - i):
                                colum.append("")
                            i= value_index
                        #Control para celda que no tiene datos
                        try:
                            value= c.find('doc:Data', ns)
                            colum.append(value.text)
                        except AttributeError:
                            colum.append("")
                    i+= 1
                #Se agregan los valores de cada fila    
                data.append(colum)
                    
        return pd.DataFrame(data)         

    def fileToJson(self,sourceFile):
        #data frame estructurar en JSON
        #Crear tabla en monet db
        dataFrame = self.loadXMLFile(sourceFile)
        dfContent = self.loadXMLFile(sourceFile, 2)
        # print(dataFrame)
        
        tablesMetadata = {}
        lastTableName = "a"
        #Convertir dataFrame en JSON estructurado
        for index, row in dataFrame.iterrows():
            if lastTableName != row[8]:
                lastTableName = row[8]
            if index > 1 and row[8] is not None and row[8] != '':
                
                if row[8] not in tablesMetadata:
                    tablesMetadata[row[8]] = []
                equivalentType = self.dbTypeEquivalence[row[5]]
                defaultDecimal= "0" if row[5] == "Number" else "0"
                length= row[6].strip() if ( row[6] is not  None and row[6] != "unrestricted" ) else self.dbLengthTypeEquivalence[row[5]]
                decimal= row[7].strip() if row[7] is not  None else defaultDecimal
                fieldDescription = {  "columnName": row[9], 
                                      "key": ('X' if row[2] == 'Key' else ''), 
                                      "type": equivalentType,
                                      "length": int(length),
                                      "decimal": int(decimal)
                                    }
                tablesMetadata[row[8]].append(fieldDescription)
            else:
                pass            
        
        # jsonTablesMetadata = json.dumps(tablesMetadata, indent=4)
        # with open(r"C:\temp\tablesMetadata.json","w") as outfile:
        #     outfile.write(jsonTablesMetadata)
        print(tablesMetadata)
        oData = []
        oData.append(tablesMetadata)
        oData.append(dfContent)
        return oData

    def createTableFromFile(self,sourceFile):
        
        tablesMetadata = self.fileToJson(self,sourceFile)
        #Crear tabla
        dbConnection = dbConnectionEngine()
        dbConnection.createTable(tablesMetadata)
        
