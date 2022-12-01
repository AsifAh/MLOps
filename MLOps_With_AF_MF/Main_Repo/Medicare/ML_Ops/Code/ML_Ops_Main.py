import json
import pandas as pd
import Packages.Do_Training as DT
from sklearn.model_selection import train_test_split

import os
from pathlib import Path
import sqlite3

Parent = Path(__file__).resolve().parents[4]
db_url= str(Parent)+'/Sqlite/WorkflowDB.db'
cnx = sqlite3.connect(db_url)
LOB = 'Medicare'
mod_string = "select * from '{}'"
x_mod_string = mod_string.format('Medicare_X_Train')
y_mod_string = mod_string.format('Medicare_Y_Train')

X_mod = pd.read_sql(x_mod_string , cnx)
y_mod = pd.read_sql(y_mod_string , cnx)
print(X_mod.shape,y_mod.shape)

X_mod.drop('Project',axis =1 ,inplace=True)
y_mod.drop('Project',axis =1 ,inplace=True)

X_mod.fillna(0,inplace=True)
y_mod.fillna(0,inplace=True)

for i in X_mod.isnull().sum():
    print(i)
print(y_mod.isnull().sum())

X_train, X_test, y_train, y_test = train_test_split(X_mod, y_mod, stratify=y_mod, test_size=0.20)

decile = DT.do_training(X_mod, X_test, y_mod, y_test, LOB)

print(decile)