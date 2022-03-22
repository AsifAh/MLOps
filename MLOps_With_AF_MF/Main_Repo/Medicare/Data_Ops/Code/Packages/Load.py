print('Inside Load')

import pandas as pd
from pathlib import Path
import sqlite3

Parent = Path(__file__).resolve().parents[5]

print(Parent)
db_url= str(Parent)+'/Sqlite/WorkflowDB.db'
db_url_2 = str(Parent)+'/Sqlite/mlops.db'
#process_url = str(Parent)+'/Sqlite/DataOps.db'
print(db_url)

# Create your connection.
cnx = sqlite3.connect(db_url)
cnx2 = sqlite3.connect(db_url_2)

def load_table(tab,wrk_id):
    p2 = pd.read_sql('select * from %s' %tab, cnx)
    print(p2.shape)

    if p2.shape[0] > 10:
        with cnx2:
            cur = cnx2.cursor()
            qry = "UPDATE dataops SET trigger_date_time = CURRENT_TIMESTAMP , run_status  = 1 " \
                  "where process_name = 'Load' and workflow_id = '{}'".format(wrk_id)
            print(qry)
            cur.execute(qry)
    else:
        with cnx2:
            cur = cnx2.cursor()
            qry = "UPDATE  dataops SET trigger_date_time = CURRENT_TIMESTAMP , run_status = 3 " \
                  "where process_name = 'Load' and workflow_id = '{}'".format(wrk_id)
            cur.execute(qry)
    if cnx:
        cnx.close()
    if cnx2:
        cnx2.close()

    return p2

