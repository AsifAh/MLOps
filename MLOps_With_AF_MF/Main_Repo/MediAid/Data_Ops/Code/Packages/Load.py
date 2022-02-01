print('Inside Load')

import pandas as pd
from pathlib import Path
import sqlite3

Parent = Path(__file__).resolve().parents[5]

print(Parent)
db_url= str(Parent)+'/Sqlite/WorkflowDB.db'
#process_url = str(Parent)+'/Sqlite/DataOps.db'
print(db_url)

# Create your connection.
cnx = sqlite3.connect(db_url)

def load_table(tab,LOB):
    p2 = pd.read_sql('select * from %s' %tab, cnx)
    print(p2.shape)

    if p2.shape[0] > 10:
        with cnx:
            cur = cnx.cursor()
            qry = "UPDATE DataOps SET Timestamp = CURRENT_TIMESTAMP , Run_Status = 1 " \
                  "where Process_name = 'Load' and Project_name = '{}'".format(LOB)
            #print(qry)
            cur.execute(qry)
    else:
        with cnx:
            cur = cnx.cursor()
            qry = "UPDATE DataOps SET Timestamp = CURRENT_TIMESTAMP , Run_Status = 3 " \
                  "where Process_name = 'Load' and Project_name = '{}'".format(LOB)
            cur.execute(qry)
    if cnx:
        cnx.close()

    return p2

