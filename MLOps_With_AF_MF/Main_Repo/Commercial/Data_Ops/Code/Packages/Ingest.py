print('Inside Ingest')

import pandas as pd
from pathlib import Path
import sqlite3

#print(1,Path(__file__).resolve().parents[0])
Parent = Path(__file__).resolve().parents[3]

print(Parent)
db_url= str(Parent)+'/Sqlite/WorkflowDB.db'
#process_url = str(Parent)+'/Sqlite/DataOps.db'
print(db_url)


# Create your connection.
cnx = sqlite3.connect(db_url)

def load_table(X,Y,LOB):
    X['Project'] = LOB
    Y['Project'] = LOB
    table_name_x = LOB+'_X_Train'
    table_name_y = LOB+'_Y_Train'
    try :
        X.to_sql(name=table_name_x, con=cnx,index=False,if_exists='replace')
        Y.to_sql(name=table_name_y, con=cnx,index=False,if_exists='replace')

        with cnx:
            cur = cnx.cursor()
            qry = "UPDATE DataOps SET Timestamp = CURRENT_TIMESTAMP , Run_Status = 1 " \
                  "where Process_name = 'Intake' and Project_name = '{}'".format(LOB)
            print(qry)
            cur.execute(qry)
        if cnx:
            cnx.close()
        return True
    except Exception as e:
        with cnx:
            cur = cnx.cursor()
            qry = "UPDATE DataOps SET Timestamp = CURRENT_TIMESTAMP , Run_Status = 3 " \
                  "where Process_name = 'Intake' and Project_name = '{}'".format(LOB)
            print(qry)
            cur.execute(qry)
        if cnx:
            cnx.close()

        return False
