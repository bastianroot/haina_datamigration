
from sqlalchemy import create_engine, select, func
from dao import dbConnectionEngine
from parsing import parsingData

dbConnection = dbConnectionEngine()
parsing = parsingData()

# parsing.createTableFromFile(r"C:\temp\Source data for Exchange rate Prueba2.xml")
oData = parsing.fileToJson(r"C:\temp\Source data for Exchange rate Prueba2.xml")
# dbConnection.readFile(r"C:\temp\Source data for Customer.xml")
# dbConnection.getDBConnection('monetdb','192.168.105.51','50000','pymonetdb','migration','monetdb','monetdb',True)
dbConnection.getDBConnection('hana','192.168.18.21','30613','hana','DS4','STG_HAINA_USER','Sistema01*',True)
# dbTables = dbConnection.getSchemaTables('migration', False, True)
dbTables = dbConnection.findTableNameonDB(oData)
# dbConnection.insertDatatoDB(oData,dbTables)

