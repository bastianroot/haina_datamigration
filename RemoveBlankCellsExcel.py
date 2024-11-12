import xml.etree.ElementTree as ET
import pandas as pd
import os

class RemoveBlankCellExcel:

    def __init__(self):
        self._sourcePath = "C:/temp/ERP/Blc de inventario al 28 Febrero/"
        self._targetPath = "C:\\temp\\ERP\\Blc de inventario al 28 Febrero\\"
    
    def ReadFile(self):
        
        file_list = os.listdir(self._sourcePath)
        
        columnsName = ['Código','Descripción','Cantidad', 'CostoRD', 'CostoUS', 'STotalRD', 'STotalUS' ]

        for file in file_list:
            fileName = f"{self._sourcePath}{file}"
            #Namespace para Excel
            ns = {"doc": "urn:schemas-microsoft-com:office:spreadsheet"}

            if ".xls" not in fileName.lower():
                continue

            try:
                tree= ET.parse(fileName)
                root= tree.getroot()

                ws= root.findall('.//doc:Worksheet', ns) #Busca todos los Worksheet
                row= ws[0].findall('.//doc:Row', ns) #Busca solamente la segunda hoja
            except ( Exception, PermissionError, ET.ParseError) as error:
                print("Error leyendo el archivo: ",fileName, error )
                continue

            data= []

            for k, r in enumerate(row):
                if k > 8:                
                    cell= r.findall('doc:Cell', ns)
                    colum= []
                    i=1 #Contador de columnas
                    for c in cell:
                        #Ver si viene Index                    
                        index= c.attrib
                        
                        #Validar existencia Index
                        index_cell = "{" + f"{ns['doc']}" + "}" + "Index"
                        if index_cell not in index:
                            #Si no existe, agrega el dato
                            value= c.find('doc:Data', ns)
                            if value is not None:
                                colum.append(value.text)                     
                        else:
                            #Si existe, agrega cantidad de espacios                        
                            # value_index= int(index["{" + f"{ns['doc']}" + "}" + "Index"])
                            # if i < value_index:
                            #     for j in range(value_index - i):
                            #         colum.append("")
                            #     i= value_index
                            #Control para celda que no tiene datos
                            try:
                                value= c.find('doc:Data', ns)
                                if value is not None and value.text != None:
                                    colum.append(value.text)
                            except AttributeError:
                                colum.append("")
                        i+= 1
                    #Se agregan los valores de cada fila   
                    if len(colum) > 0:
           
                        if len(colum) == 1:
                            data.append(colum)
                        else:
                            try:
                                FoundValue = colum.index('Totales -->')
                            except ValueError:
                                FoundValue = -1

                            if FoundValue == -1:
                                for columnValue in colum:                                
                                    if columnValue not in columnsName:
                                        data[(len(data) - 1)].append(columnValue)
                                    else:
                                        print("")

            filePath = f"{self._targetPath}CO_{file}"
            print(f"Escribir archivo: {filePath}")
            df = pd.DataFrame(data, columns = columnsName )            
            df.to_excel(filePath,index=False)
        print("Proceso finalizado")

ProcessFile = RemoveBlankCellExcel()
ProcessFile.ReadFile()