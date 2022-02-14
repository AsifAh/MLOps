print('Inside do Training')

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






