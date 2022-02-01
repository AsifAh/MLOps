print('Inside Ingest')

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

def load_table(X,Y,LOB):
    X['Project'] = LOB
    Y['Project'] = LOB
    try :
        X.to_sql(name='X_Train', con=cnx,index=False,if_exists='replace')
        Y.to_sql(name='Y_Train', con=cnx,index=False,if_exists='replace')

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
