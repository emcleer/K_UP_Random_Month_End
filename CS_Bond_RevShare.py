import os
import pandas as pd

walk_dir = r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\Checks_PEL\2016'

df_list = []

for root, subdirs, files in os.walk(walk_dir):
    for f in files:
        df = pd.read_csv(os.path.join(root, f))
        df_list.append(df)
        
total = pd.concat(df_list, axis=0)

csos = total[(total['Trans.Agent Acct.'] == 'CSOS') & (total['Cash BOI Fee Amount'] > 0)].copy()
csos['Check Date'] = csos['Check Date'].astype(str)
csos['Check Date'] = pd.to_datetime(csos['Check Date'])
csos['Month'] = csos['Check Date'].apply(lambda x: x.strftime('%Y%m'))

csos.to_clipboard()



#grp = csos.groupby('Month')['Cash BOI Fee Amount'].sum()
#grp.reset_index().to_clipboard()
