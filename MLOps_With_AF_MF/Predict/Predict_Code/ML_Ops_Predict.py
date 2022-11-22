import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import sys
import sqlite3

from os.path import dirname as up

import joblib

predict_path = os.environ['PREDICTPATH']
print(predict_path)

os.chdir(predict_path)

db_url= './Sqlite/WorkflowDB.db'
cnx = sqlite3.connect(db_url)

LOB = os.environ['lob']
print(LOB)

mod_string = "select * from '{}_WOE'"
x_mod_string = mod_string.format(LOB)
woe_df = pd.read_sql(x_mod_string , cnx)

print('--->', woe_df.shape)


LOB_list = [LOB]
Map_file_name = "ICD_HCPCS_CPT_Codes_Map.xlsx"

test_df =pd.read_excel('./Predict/Daily_Feed/Daily_Feed.xlsx')

db_url= './Sqlite/mlruns.db'
cnx2 = sqlite3.connect(db_url)
source = "select source from model_versions where name ='{}' and current_stage= 'Production' "
x_mod_source = source.format(LOB)
source_df = pd.read_sql(x_mod_source , cnx2)
#print(source_df.columns)
try :
    print('~~~~~~~~',source_df['source'].iloc[0])

    stacked_from_joblib = joblib.load(source_df['source'].iloc[0] + '/model.pkl')
except:
    print("No Models found in production")
    sys.exit()

def read_files(Map_file_name):
    icd_map_df = pd.read_excel('./Map_Files/' + Map_file_name, sheet_name="ICD-CM-BILL")
    cpt_map_df = pd.read_excel('./Map_Files/' + Map_file_name, sheet_name="CPT-CODES")
    drg_map_df = pd.read_excel('./Map_Files/' + Map_file_name, sheet_name="DRG")
    tos_map_df = pd.read_excel('./Map_Files/' + Map_file_name, sheet_name="TOS-CODES")
    pos_map_df = pd.read_excel('./Map_Files/' + Map_file_name, sheet_name="POS-CODES")

    return icd_map_df, cpt_map_df, drg_map_df, tos_map_df, pos_map_df


def train_init(test_df, icd_map_df, cpt_map_df, drg_map_df, tos_map_df, pos_map_df, LOB_list):
    y_pred_list = []
    for LOB in LOB_list:
        pred_df = test_df[test_df['LOB'] == LOB]
        print(pred_df.shape)
        # map for age and POS
        pred_df['Age_Group'] = pd.cut(x=pred_df['MEM_AGE'],
                                      bins=[0, 10, 20, 30, 50, 60, 100],
                                      labels=['Children', 'Adolescent', 'Teenage', 'Adults', 'Older-Adults',
                                              'Senior-Citizen'])
        pred_df['Age_Group'] = pred_df['Age_Group'].astype(str)
        #print(pred_df.shape)
        pred_df = pred_df.merge(icd_map_df[['ICD_CM_Codes', 'ABS']], how='left', left_on='IDCD_ID',
                                right_on='ICD_CM_Codes').drop_duplicates()
        pred_df.rename(columns={'ABS': 'ICD_MAP'}, inplace=True)
        #print(pred_df.shape)
        pred_df = pred_df.merge(cpt_map_df[['Codes', 'Desc']], how='left', left_on='CPT_HCPC',
                                right_on='Codes').drop_duplicates()
        pred_df.rename(columns={'Desc': 'CPT_MAP'}, inplace=True)
        #print(pred_df.shape)
        pred_df = pred_df.merge(drg_map_df[['Codes', 'Desc']], how='left', left_on='DRG_CODE',
                                right_on='Codes').drop_duplicates()
        pred_df.rename(columns={'Desc': 'DRG_MAP'}, inplace=True)
        #print(pred_df.shape)
        pred_df = pred_df.merge(tos_map_df, how='left', left_on='TOS', right_on='TOS').drop_duplicates()
        pred_df.rename(columns={'Tos_Desc': 'TOS_MAP'}, inplace=True)
        #print(pred_df.shape)
        pred_df = pred_df.merge(pos_map_df, how='left', on='POS').drop_duplicates()
        pred_df.rename(columns={'POS_Name': 'POS_MAP'}, inplace=True)
        #print(pred_df.shape)
        pred_df['DRG_MAP'] = pred_df['DRG_MAP'].fillna("Others")
        #print(pred_df.shape)
        #woe_df = pd.read_csv('./Buffer_Files/WOE.csv')

        col_name = woe_df['Variable_Name'].unique().tolist()
        feature_cols = []
        for i in col_name:
            if i != 'REVENUE_CODE':
                # print(i)
                temp_df = woe_df[woe_df['Variable_Name'] == i]
                # print(temp_df.shape)
                # temp_df.drop('Variable_Name')
                temp_df.drop(['Variable_Name'], axis=1, inplace=True)
                temp_df.columns = [i + '_' + x for x in temp_df.columns]
                temp_feat = list(temp_df.columns[1:])
                feature_cols.extend(temp_feat)
                pred_df[i] = pred_df[i].astype(str)
                pred_df = pred_df.merge(temp_df, how='left', left_on=i, right_on=i + '_' + 'Category')
                # print(pred_df.columns)
            else:
                pass
        feature_cols

        X_test = pred_df[feature_cols]
        print(X_test.shape)
        # Load the model from the file
        #stacked_from_joblib = joblib.load('./Buffer_Files/' + LOB + '.pkl')

        y_pred = stacked_from_joblib.predict_proba(X_test)[:, 1]
        pred_df['Final_Predictions'] = y_pred

        test_df = test_df.merge(pred_df[['HC_CLAIM_ID', 'SEQ', 'LOB', 'Final_Predictions']], how='left',
                                on=['HC_CLAIM_ID', 'SEQ', 'LOB'])
        print(len(y_pred))
        y_pred_list.extend(list(y_pred))
    return y_pred_list, test_df


icd_map_df,cpt_map_df,drg_map_df,tos_map_df,pos_map_df = read_files(Map_file_name)
predictions,final_df=train_init(test_df,icd_map_df,cpt_map_df,drg_map_df,tos_map_df,pos_map_df,LOB_list)

#final_df['v'] = final_df[['Predictions_x', 'Predictions_y', 'Predictions']].max(axis=1)
#final_df.drop(['Predictions_x', 'Predictions_y', 'Predictions'],axis=1, inplace=True)

final_df.sort_values('Final_Predictions',ascending=False,inplace=True)
final_df["Rank"] = np.arange(final_df.shape[0])
final_df["Quantile"] = pd.qcut(final_df["Rank"], q=10,labels=['Q1','Q2','Q3','Q4','Q5',
                                      'Q6','Q7','Q8','Q9','Q10'])

final_df["Risk Category"] = final_df["Quantile"].apply(lambda x : "High Risk" if x in ['Q1','Q2','Q3'] else "Low Risk")

final_df.drop(['Rank', 'Quantile'],axis=1, inplace=True)

final_df.to_excel('./Predict/Predictions/Daily_predictions.xlsx',index=False)

print('DataFrame is written to Excel File successfully.')