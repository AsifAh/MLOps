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

Parent = Path(__file__).resolve().parents[4]
db_url= str(Parent)+'/Sqlite/WorkflowDB.db'
cnx = sqlite3.connect(db_url)
Load_df = Load.load_table('MediAid_Historic_Data',"MediAid")
status = pd.read_sql("select * from DataOps " \
                     "where Process_name = 'Load' and Project_name = 'MediAid'", cnx)
if  status[['Run_Status']].values[0][0] == 1:
    icd_map_df, cpt_map_df, drg_map_df, tos_map_df, pos_map_df = Cleanse.read_files(Map_file_name,'MediAid')
else:
    print('no')

status = pd.read_sql("select * from DataOps " \
                     "where Process_name = 'Cleanse' and Project_name = 'MediAid'", cnx)

if  status[['Run_Status']].values[0][0] == 1:
    X_mod,y_mod = Transform.Transform(Load_df, icd_map_df, cpt_map_df, drg_map_df, tos_map_df, pos_map_df, 'MediAid')
else:
    print('no')


status = pd.read_sql("select * from DataOps " \
                     "where Process_name = 'Transform' and Project_name = 'MediAid'", cnx)

if  status[['Run_Status']].values[0][0] == 1:
    Status = Ingest.load_table(X_mod,y_mod,'MediAid')
else:
    print('no')

if Status :
    print(True)
else:
    print(False)

