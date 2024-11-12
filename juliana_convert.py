import jdcal
from datetime import date,datetime, timedelta

def normal_date(jd, fecha_minima):
    jd= jd + 1721424.5
    np= jdcal.jd2gcal(jd, 0)
    
    #Filtro para fecha mínima
    if np[0] >= fecha_minima:
        date_doc_str = str(np[2]).zfill(2) + "." + str(np[1]).zfill(2) + "." + str(np[0])
        date_doc = datetime.strptime( date_doc_str, '%d.%m.%Y').date()
        # return str(np[2]).zfill(2) + "." + str(np[1]).zfill(2) + "." + str(np[0])
        return date_doc
    else:
        return ""