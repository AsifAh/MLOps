print('Inside Cleanse')

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

def read_files(Map_file_name,LOB):
    try:

        icd_map_df = pd.read_excel(str(Parent)+'/Map_Files/' + Map_file_name, sheet_name="ICD-CM-BILL",engine='openpyxl')
        cpt_map_df = pd.read_excel(str(Parent)+'/Map_Files/' + Map_file_name, sheet_name="CPT-CODES",engine='openpyxl')
        drg_map_df = pd.read_excel(str(Parent)+'/Map_Files/' + Map_file_name, sheet_name="DRG",engine='openpyxl')
        tos_map_df = pd.read_excel(str(Parent)+'/Map_Files/' + Map_file_name, sheet_name="TOS-CODES",engine='openpyxl')
        pos_map_df = pd.read_excel(str(Parent)+'/Map_Files/' + Map_file_name, sheet_name="POS-CODES",engine='openpyxl')

        with cnx2:
            cur = cnx2.cursor()
            qry = "UPDATE dataops SET trigger_date_time = CURRENT_TIMESTAMP , run_status  = 1 " \
                  "where process_name = 'Cleanse' and workflow_id = '{}'".format(LOB)
            print(qry)
            cur.execute(qry)
        if cnx2:
            cnx2.close()
        if cnx:
            cnx.close()
        return icd_map_df, cpt_map_df, drg_map_df, tos_map_df, pos_map_df
    except Exception as e :
        print(e)
        with cnx2:
            cur = cnx2.cursor()
            qry = "UPDATE dataops SET trigger_date_time = CURRENT_TIMESTAMP , run_status  = 3 " \
                  "where process_name = 'Cleanse' and workflow_id = '{}'".format(LOB)
            cur.execute(qry)
        if cnx2:
            cnx2.close()
        if cnx:
            cnx.close()
        return False




