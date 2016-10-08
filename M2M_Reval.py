import pandas as pd

#Change pending report file when running for next month
eom = pd.read_csv(r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\PEL\2016-09-30.csv')

#Change .csv file name to save for new month
fp_out = r'H:\MonthEnd\201609_M2M_Reval.csv'

def reval(row):
    if row['Job#'] == 5001:
        if row['Proc.Fee %'] == 10:
            if row['Tot.Tendered Shares'] * row['Strike Amount'] > 20000:
                x = row['Tot.Tendered Shares']
                y = 20000 / row['Strike Amount']     #amount of shares to be applied @ 10%
                z = x - y                            #remaining shares to be applied @ 5%
                return ((row['Strike Amount'] * y * .10) + (row['Strike Amount'] * z * .05))
            else:
                return (row['Strike Amount'] * row['Tot.Tendered Shares'] * .10)
        else:
            return (row['Strike Amount'] * row['Tot.Tendered Shares'] * row['Proc.Fee %'] / 100)
    else:
        return (row['Strike Amount'] * row['Tot.Tendered Shares'] * row['Proc.Fee %'] / 100)
       
def final(df):
    msk = df['Proc.Fee $'] > 0
    df2 = df[msk].copy()
    df2['m2m'] = df2.apply(reval, axis=1)
    df2['reval'] = df2['m2m'] - df2['Proc.Fee $']
    return df2[['Job#', 'LT#', 'Case Number', 'Report#', 'Trans.Agent Acct.','Process Date', 
                'Tot.Tendered Shares', 'Strike Amount', 'Proc.Fee %', 'Proc.Fee $', 'm2m', 'reval']]

final(eom).to_csv(fp_out, index=False)