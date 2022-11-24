print('Inside Ingest')

import pandas as pd
from pathlib import Path
import sqlite3

Parent = Path(__file__).resolve().parents[5]

print(Parent)
db_url= str(Parent)+'/Sqlite/WorkflowDB.db'
#process_url = str(Parent)+'/Sqlite/DataOps.db'
print(db_url)

db_url_2 = str(Parent)+'/Sqlite/mlops.db'
cnx2 = sqlite3.connect(db_url_2)

# Create your connection.
cnx = sqlite3.connect(db_url)

def load_table(X,Y,W,LOB,wrk_id):
    X['Project'] = LOB
    Y['Project'] = LOB
    
    print("WWW" , W.shape)
    W=W.astype(str)
    table_name_x = LOB+'_X_Train'
    table_name_y = LOB+'_Y_Train'
    table_name_woe = LOB+'_WOE'
    try :
        W.to_sql(name=table_name_woe, con=cnx, index=False, if_exists='replace')
        X.to_sql(name=table_name_x, con=cnx,index=False,if_exists='replace')
        Y.to_sql(name=table_name_y, con=cnx,index=False,if_exists='replace')

        with cnx2:
            cur = cnx2.cursor()
            qry = "UPDATE dataops SET trigger_date_time = CURRENT_TIMESTAMP , run_status  = 1 " \
                  "where process_name = 'Intake' and workflow_id = '{}'".format(wrk_id)
            print(qry)
            cur.execute(qry)
        if cnx2:
            cnx2.close()
        if cnx:
            cnx.close()
        return True
    except Exception as e:
        print(e)
        with cnx2:
            cur = cnx2.cursor()
            qry = "UPDATE dataops SET trigger_date_time = CURRENT_TIMESTAMP , run_status  =  3 " \
                  "where process_name = 'Intake' and workflow_id = '{}'".format(wrk_id)
            print(qry)
            cur.execute(qry)
        if cnx2:
            cnx2.close()
        if cnx:
            cnx.close()

        return False
