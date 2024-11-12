import xml.etree.ElementTree as ET
import pandas as pd

file= r"C:\Users\Wilson\Desktop\test2.xml"
file_json= r"C:\Users\Wilson\Desktop\JSONtest.json"

#Namespace para Excel
ns = {"doc": "urn:schemas-microsoft-com:office:spreadsheet"}

tree= ET.parse(file)
root= tree.getroot()

ws= root.findall('.//doc:Worksheet', ns) #Busca todos los Worksheet
row= ws[1].findall('.//doc:Row', ns) #Busca solamente la segunda hoja

data= []

for k, r in enumerate(row):
    if k > 2:
        print(r.tag, r.attrib)
        cell= r.findall('doc:Cell', ns)
        colum= []
        i=1 #Contador de columnas
        for c in cell:
            #Ver si viene Index
            print(c.attrib)
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
                print("found")
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
            
df= pd.DataFrame(data)


        
