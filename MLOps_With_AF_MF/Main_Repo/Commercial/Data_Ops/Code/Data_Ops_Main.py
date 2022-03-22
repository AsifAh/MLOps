import json
import pandas as pd
import Packages.Ingest as Ingest
import Packages.Transform as Transform
import Packages.Cleanse as Cleanse
import Packages.Load as Load

import os
from pathlib import Path
import sqlite3

Map_file_name = "ICD_HCPCS_CPT_Codes_Map.xlsx"
workFlowId = os.environ['workFlowId']
print(workFlowId)

Parent = Path(__file__).resolve().parents[4]
db_url= str(Parent)+'/Sqlite/WorkflowDB.db'
db_url_2 = str(Parent)+'/Sqlite/mlops.db'

cnx2 = sqlite3.connect(db_url_2)
cnx = sqlite3.connect(db_url)
Load_df = Load.load_table('Commercial_Historic_Data',workFlowId)
qry_load = "select * from dataops " \
      "where process_name = 'Load' and workflow_id = '{}'".format(workFlowId)

status = pd.read_sql(qry_load , cnx2)
if  status[['run_status']].values[0][0] == 1:
    icd_map_df, cpt_map_df, drg_map_df, tos_map_df, pos_map_df = Cleanse.read_files(Map_file_name,workFlowId)
else:
    print('no')

qry_clean = "select * from dataops " \
      "where process_name = 'Cleanse' and workflow_id = '{}'".format(workFlowId)

status = pd.read_sql(qry_clean, cnx2)

if  status[['run_status']].values[0][0] == 1:
    X_mod,y_mod = Transform.Transform(Load_df, icd_map_df, cpt_map_df, drg_map_df, tos_map_df, pos_map_df, workFlowId)
else:
    print('no')

qry_trans = "select * from dataops " \
      "where process_name = 'Transform' and workflow_id = '{}'".format(workFlowId)

status = pd.read_sql(qry_trans, cnx2)

if  status[['run_status']].values[0][0] == 1:
    Status = Ingest.load_table(X_mod,y_mod,'Commercial',workFlowId)
else:
    print('no')

if Status :
    print(True)
else:
    print(False)