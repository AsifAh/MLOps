print('Inside Transform')
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from pathlib import Path
from xverse.transformer import WOE

import sqlite3

Parent = Path(__file__).resolve().parents[5]

print(Parent)
db_url= str(Parent)+'/Sqlite/WorkflowDB.db'
db_url_2 = str(Parent)+'/Sqlite/mlops.db'
cnx2 = sqlite3.connect(db_url_2)
#process_url = str(Parent)+'/Sqlite/DataOps.db'
print(db_url)

# Create your connection.
cnx = sqlite3.connect(db_url)


def Transform(raw_df,icd_map_df, cpt_map_df, drg_map_df, tos_map_df, pos_map_df,LOB):

    # Error definition
    print(raw_df.dtypes)
    raw_df['Error'] = raw_df['ADJ_INT_AMT'].apply(lambda x: 1 if float(x) > 0.00 else 0)
    # map for age and POS
    raw_df['Age_Group'] = pd.cut(x=raw_df['MEM_AGE'],
                                 bins=[0, 10, 20, 30, 50, 60, 100],
                                 labels=['Children', 'Adolescent', 'Teenage', 'Adults', 'Older-Adults',
                                         'Senior-Citizen'])

    raw_df['Age_Group'] = raw_df['Age_Group'].astype(str)
    # Following columns needs remapping
    print(drg_map_df.dtypes)
    raw_df = raw_df.merge(icd_map_df[['ICD_CM_Codes', 'ABS']], how='left', left_on='IDCD_ID',
                          right_on='ICD_CM_Codes')
    raw_df.rename(columns={'ABS': 'ICD_MAP'}, inplace=True)
    raw_df = raw_df.merge(cpt_map_df[['Codes', 'Desc']], how='left', left_on='CPT_HCPC', right_on='Codes')
    raw_df.rename(columns={'Desc': 'CPT_MAP'}, inplace=True)
    raw_df = raw_df.merge(drg_map_df[['Codes', 'Desc']], how='left', left_on='DRG_CODE', right_on='Codes')
    raw_df.rename(columns={'Desc': 'DRG_MAP'}, inplace=True)
    raw_df = raw_df.merge(tos_map_df, how='left', left_on='TOS', right_on='TOS')
    raw_df.rename(columns={'Tos_Desc': 'TOS_MAP'}, inplace=True)
    raw_df = raw_df.merge(pos_map_df, how='left', on='POS')
    raw_df.rename(columns={'POS_Name': 'POS_MAP'}, inplace=True)

    raw_df['DRG_MAP'] = raw_df['DRG_MAP'].fillna("Others")

    features = ['PLACEOFSERVICE', 'Age_Group', 'USERID', 'MEME_SEX', 'MEME_MARITAL_STATUS', 'AUTH_INDICATOR',
                'REFFERAL_INDICATOR', 'COB_IND', 'CAP_IND',
                'SERVICING_PROVIDER_ID', 'REVENUE_CODE', 'CPT_MAP', 'ICD_MAP', 'DRG_MAP', 'POS_MAP', 'TOS_MAP']
    target = ['Error']

    # raw_df[features[:2]]
    X = raw_df[features]
    y = raw_df[target].T.squeeze()

    clf = WOE()
    clf.fit(X, y)

    woe_df = clf.woe_df

    # woe_df.to_csv('./Buffer_Files/Temp/WOE.csv', index=False)
    # woe_path = './Buffer_Files/Temp/'
    col_name = woe_df['Variable_Name'].unique().tolist()
    feature_cols = []
    for i in col_name:
        if i != 'REVENUE_CODE':
            print(i)
            temp_df = woe_df[woe_df['Variable_Name'] == i]
            # print(temp_df.shape)
            # temp_df.drop('Variable_Name')
            temp_df.drop(['Variable_Name'], axis=1, inplace=True)
            temp_df.columns = [i + '_' + x for x in temp_df.columns]
            temp_feat = list(temp_df.columns[1:])
            feature_cols.extend(temp_feat)
            raw_df[i] = raw_df[i].astype(str)
            raw_df = raw_df.merge(temp_df, how='left', left_on=i, right_on=i + '_' + 'Category')
            # print(raw_df.columns)
        else:
            pass

    # Modelling
    X_mod = raw_df[feature_cols]
    y_mod = raw_df[target]

    if X_mod.shape[0] > 10:
        with cnx2:
            cur = cnx2.cursor()
            qry = "UPDATE dataops SET trigger_date_time = CURRENT_TIMESTAMP , run_status  = 1 " \
                  "where process_name = 'Transform' and workflow_id = '{}'".format(LOB)
            print(qry)
            cur.execute(qry)
    else:
        with cnx2:
            cur = cnx2.cursor()
            qry = "UPDATE DataOps SET trigger_date_time = CURRENT_TIMESTAMP , Run_Status = 3 " \
                  "where process_name = 'Transform' and workflow_id = '{}'".format(LOB)
            cur.execute(qry)
    if cnx2:
        cnx2.close()
    if cnx:
        cnx.close()

    return X_mod,y_mod,woe_df
