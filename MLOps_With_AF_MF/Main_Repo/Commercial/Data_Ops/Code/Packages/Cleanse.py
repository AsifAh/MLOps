print('Inside Cleanse')

import pandas as pd
from pathlib import Path
import sqlite3

Parent = Path(__file__).resolve().parents[3]

print(Parent)
db_url= str(Parent)+'/Sqlite/WorkflowDB.db'
#process_url = str(Parent)+'/Sqlite/DataOps.db'
print(db_url)

Parent2 = Path(__file__).resolve().parents[5]

# Create your connection.
cnx = sqlite3.connect(db_url)

def read_files(Map_file_name,LOB):
    try:

        icd_map_df = pd.read_excel(str(Parent2)+'/Map_Files/' + Map_file_name, sheet_name="ICD-CM-BILL")
        cpt_map_df = pd.read_excel(str(Parent2)+'/Map_Files/' + Map_file_name, sheet_name="CPT-CODES")
        drg_map_df = pd.read_excel(str(Parent2)+'/Map_Files/' + Map_file_name, sheet_name="DRG")
        tos_map_df = pd.read_excel(str(Parent2)+'/Map_Files/' + Map_file_name, sheet_name="TOS-CODES")
        pos_map_df = pd.read_excel(str(Parent2)+'/Map_Files/' + Map_file_name, sheet_name="POS-CODES")

        with cnx:
            cur = cnx.cursor()
            qry = "UPDATE DataOps SET Timestamp = CURRENT_TIMESTAMP , Run_Status = 1 " \
                  "where Process_name = 'Cleanse' and Project_name = '{}'".format(LOB)
            print(qry)
            cur.execute(qry)
        if cnx:
            cnx.close()
        return icd_map_df, cpt_map_df, drg_map_df, tos_map_df, pos_map_df
    except Exception as e :
        print(e)
        with cnx:
            cur = cnx.cursor()
            qry = "UPDATE DataOps SET Timestamp = CURRENT_TIMESTAMP , Run_Status = 3 " \
                  "where Process_name = 'Cleanse' and Project_name = '{}'".format(LOB)
            cur.execute(qry)
        if cnx:
            cnx.close()
        return False




