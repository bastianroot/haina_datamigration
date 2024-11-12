from sqlalchemy import create_engine, inspect, MetaData, Table, Column, String, Numeric
from sshtunnel import SSHTunnelForwarder
import xml.etree.ElementTree as ET
import pandas as pd
import urllib.parse
import numpy as np
# import module sys to get the type of exception
import sys


class dbConnectionEngine:

    def __init__(self):
        self._dbEngineName = ""
    
    def getDBConnection(self, dbEngineName, host, port, driver, dbname, user, password,ssh_connect):
        
        local_host = '127.0.0.1'
        # if ssh_connect == True:
        #     server = SSHTunnelForwarder( (host, 222),
        #                                 ssh_username="rbtdev05",
        #                                 ssh_password="T6TZkmaiiBo1",
        #                                 remote_bind_address=(local_host, 50000))

        #     server.start()
        #     port = str(server.local_bind_port)        
        #     host = local_host
        #     # server.close()

        self._dbEngineName = dbEngineName
        # driver = '?driver='+driver if ( driver != '' )  else ''
        #driver = ''
        if dbEngineName == "monetdb":
            connection_arguments = f"{dbEngineName}://{user}:{urllib.parse.quote_plus(password)}@{host}:{port}/{dbname}{driver}"
        elif dbEngineName == "mssql":  # SQL Server
            connection_arguments = f"mssql+pymssql://{user}:{urllib.parse.quote_plus(password)}@{host}/{dbname}"#?driver={driver}"            
        elif dbEngineName == "postgresql":
            connection_arguments = f"{dbEngineName}://{user}:{urllib.parse.quote_plus(password)}@{host}/{dbname}{driver}"
        elif dbEngineName == "mysql":  # MySQL
            connection_arguments = f"{dbEngineName}://{user}:{urllib.parse.quote_plus(password)}@{host}/{dbname}{driver}"
        elif dbEngineName == "oracle":
            connection_arguments = f"{dbEngineName}://{user}:{urllib.parse.quote_plus(password)}@{host}/{dbname}{driver}"
        elif dbEngineName == "sqlite":
            connection_arguments = f"{dbEngineName}://{user}:{urllib.parse.quote_plus(password)}@{host}/{dbname}{driver}"
        elif dbEngineName == "hana":
            # "hana://username:secret-password@example.com/TENANT_NAME?encrypt=true&compress=true"
            connection_arguments = f"{dbEngineName}://{user}:{urllib.parse.quote_plus(password)}@{host}:{port}/{dbname}"
            # _, result_kwargs = sqlalchemy.testing.db.dialect.create_connect_args(
            #     make_url(connection_arguments)
            # )
            # assert result_kwargs["encrypt"] == "true"
            # assert result_kwargs["compress"] == "true"
        
        self.engine = create_engine(connection_arguments, echo=False)
        self.connection = self.engine.connect()
        return self.engine

    def findTableNameonDB(self, oData):
       
        #Nombre de todas la tablas
        inspector= inspect(self.engine)
        whereCondition = ""
        tableColumnsName = []
        tableValues = []
        for key, value in oData[0].items():
            for columnfromFile in value:
                tableColumnsName.append(columnfromFile['columnName'])
                whereCondition = f"{whereCondition}, '{columnfromFile['columnName']}'"
        whereCondition = whereCondition.replace(',','',1)
        sql_query = pd.read_sql_query(f"SELECT * FROM SYS.TABLE_COLUMNS WHERE COLUMN_NAME IN ( {whereCondition} ) ORDER BY POSITION", self.connection)
        df = pd.DataFrame(sql_query)
        print(df)
        tableName = df["table_name"][0]
        columns = inspector.get_columns(tableName)
        if ( len(value) + 2 ) ==  len(columns):
            for index, value in enumerate(oData[1].values):
                if index >= 5:
                    value[7] = round(float(value[7]),2) 
                    valueTem = value
                    valueTem = np.append(valueTem,["",""])                                   
                    tableValues.append(valueTem)

            tableColumnsName = []
            for column in columns:
                tableColumnsName.append(column["name"])
            if len(tableValues) > 0:
                dfInsert = pd.DataFrame(tableValues, columns= tableColumnsName)
                dfInsert.to_sql(tableName, con = self.engine, index = False, if_exists='append' )       
                 
        return tableName

    def getSchemaTables(self,project,withTableData):
       
        #Nombre de todas la tablas
        inspector= inspect(self.engine)
        tables= inspector.get_table_names()
        
        print(tables)
        if withTableData == True:
            for table in tables:
                
                #Consulta general a la tabla
                sql_query = pd.read_sql_query(f"select * from '{table}'", self.connection)
            
                #Conversión a tabla pandas
                df = pd.DataFrame(sql_query)
                
                characterMap = {u'\u0010': '', u'\u00E7': '', u'\u00E7': 'c', u'\u00C7' : 'C', u'\u011F' : 'g', u'\u011E' : 'G', u'\u00F6': 'o', u'\u00D6' : 'O', u'\u015F' : 's', u'\u015E' : 'S', u'\u00FC' : 'u', u'\u00DC' : 'U' , u'\u0131' : 'i', u'\u0049' : 'I', u'\u0259' : 'e', u'\u018F' : 'E'}
                
                for c in df.columns:
                    df[c] = (df[c].astype("str")
                                                .str.rstrip()
                                                .replace(characterMap, regex=True)
                                                .str.normalize('NFKD')
                                                .str.encode('ascii', errors='ignore')
                                                .str.decode('ascii'))
            
                #Creación del archivo XML
                #df.to_json(fr'C:\temp\{table}.json', index = False)

        return tables

    def createTable(self,tableMetadata):
        self.getDBConnection('monetdb','192.168.105.51','50000','pymonetdb','migration','monetdb','monetdb',True)
        metadata_obj = MetaData(bind=self.engine)

        # database name
        for table in tableMetadata:
            #Consulta general a la tabla
            tableName = table
            if tableName != "S_CUST_GEN":
                profile = Table(
                    tableName,                                        
                    metadata_obj,
                    *(Column(column['columnName'], column['type'](column['length'], column['decimal']), primary_key=True if column['key'] == 'X' else False) for column in tableMetadata[table] )
                    )

                profile.create()

    def readFile(self,sourceFile):
        file= r"C:\Users\Wilson\Desktop\test2.xml"
        file = sourceFile
        file_json= r"C:\Users\Wilson\Desktop\JSONtest.json"

        #Namespace para Excel
        ns = {"doc": "urn:schemas-microsoft-com:office:spreadsheet"}

        tree= ET.parse(file)
        root= tree.getroot()


        ws= root.findall('.//doc:Worksheet', ns) #Busca todos los Worksheet   
        
        fileData= {}
        worksheetData = {}
        fileColumns=[]        
        
        for worksheet in ws:
            wsName = worksheet.attrib[f"{{{ns['doc']}}}Name"]
            if wsName == 'Introduction' or wsName == 'Field List': continue
            row= worksheet.findall('.//doc:Row', ns) #Busca solamente la segunda hoja
                                                    
            for k, r in enumerate(row):
                cells = row[k].findall('doc:Cell', ns)
                #Nombre de columnas
                if k == 4:
                    for objColumnName in cells:
                        fileColumns.append(objColumnName.find('doc:Data', ns).text)
                if k > 4: 
                    fileData = {}           
                    for cellIndex, objCellValue in enumerate(cells):                       
                        fileData[fileColumns[cellIndex]] = objCellValue.find('doc:Data', ns).text                  
                    worksheetData[wsName] = fileData
        df= pd.DataFrame(worksheetData)
        print("Load data succeful")
        #json= df.to_json()
    
    def insertDatatoDB(self, oData, dbTables):
        pass
