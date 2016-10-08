import pandas as pd

#month-end pending report file -- need to change date
PELfp = r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\PEL\2016-09-30.csv'
MORfp = r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\MOR\2016-09-30.csv'

#output filepath of raw data
OPfp = r'H:\MonthEnd\201609_TAaccrual.csv'

def pelPend(fp):
    df = pd.read_csv(fp)
    return df

def morPend(fp):
    df = pd.read_csv(fp)
    return df

def taFuncPEL(row):
    if (row['Trans.Agent Name'] == 'AT&T/Keane Direct') & (row['Job#'] == 5001):
        return 'PEL_ATT'
    elif (row['Trans.Agent Name'] == 'AT&T/Keane Direct') & (row['Job#'] != 5001) & (row['Job#'] != 5012):
        return 'PEL_KD'
    elif row['Trans.Agent Name'] == 'Wells Fargo':
        return 'PEL_WF'
    else:
        return 'n/a'

def BOI(row):
    if row['BOI Fee Amount'] > 0:
        return 'Yes'
    else:
        return 'No'

def DivsOnly(row):
    if row['Type'] == 'A':
        return 'Yes'
    else:
        return 'No'

def formatPELAccrual(df):
    df['tatype'] = df.apply(taFuncPEL, axis = 1)
    df['BOI'] = df.apply(BOI, axis = 1)
    df['DivsOnly'] = df.apply(DivsOnly, axis=1)
    return df

def PELoutput():
    df = formatPELAccrual(pelPend(PELfp))
    msk = df['tatype'] != 'n/a'
    df2 = df[msk][['Job#', 'LT#','Report#', 'BOI', 'DivsOnly','tatype']].copy()
    return df2

def formatMORAccrual():
    df = morPend(MORfp)
    df['tatype'] = ['MOR_ATT' if x in[996005,996020,996045,996954] else 'n/a' for x in df['Job#']]
    df['BOI'] = 'n/a'
    df['DivsOnly'] = 'n/a'
    return df
    
def MORoutput():
    df = formatMORAccrual()
    msk = df['tatype'] != 'n/a'
    df2 = df[msk][['Job#','Report#', 'LT#', 'BOI', 'DivsOnly','tatype']].copy()
    return df2
            
def finalDF():
    df1, df2 = PELoutput(), MORoutput()
    final = pd.concat([df1, df2],ignore_index=True)
    return final

finalDF().to_csv(OPfp, index=False)