import pandas as pd
import glob
import sqlite3

#run from command line

def main():
    dtes = {
        'last_mo_pend' : '2016-08-31',    #pending report name
        'curr_mo_pend' : '2016-09-30',    #pending report name
        'curr_mo_chks' : '201609'         #check file folder path
    }
    
    db = r'I:\FINANCE\ACCOUNTING\Maranon Reports\2015\New Keane 13-wk Cash\OperatingMetrics\submittals.sqlite'
    conn = sqlite3.connect(db)
    df = final_frame(dtes, conn)
    df.to_csv('PEL to LCS Transfer_{0}.csv'.format(dtes['curr_mo_chks']), index=False)
    
    
def chks(d):
    clist = []
    for cfile in glob.glob(r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\Checks_PEL\2016\{}\*.csv'.format(d['curr_mo_chks'])):
        cdt = cfile.split("\\")[-1][2:12]
        if cdt >= "2016-05-01":
            cdf = pd.read_csv(cfile)
            clist.append(cdf) ##store dataframe in list
    checks = pd.concat(clist, axis=0).reset_index()
    checks['rev_amt'] = checks['Cash BOI Fee Amount'] + checks['Cash Proc.Fee Amount'] + checks['Div.Proc.Fee Amount']
    checks.rename(columns={'Job#': 'job', 'LT#': 'lt', 'Keane"s case#': 'case_number', 'Check Date': 'check_dte', 'Trans.Agent Name': 'ta'}, inplace=True)
    grp = checks.groupby('lt').rev_amt.sum()
    df = pd.DataFrame(grp).reset_index()
    checks2 = checks.drop_duplicates('lt')
    df2 = pd.merge(df, checks2[['job', 'case_number', 'check_dte', 'ta', 'lt']], how='left', on='lt')
    return df2

def last_mo(d):
    df = pd.read_csv(r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\PEL\{0}.csv'.format(d['last_mo_pend']))
    df['rev_amt'] = df['BOI Fee Amount'] + df['Proc.Fee $'] + df['Proc.Fee Amount "DIV"']
    df.rename(columns={'Job#': 'job', 'LT#': 'lt', 'Case Number': 'case_number', 'Process Date': 'sub_dte', 'Trans.Agent Name': 'ta'}, inplace=True)
    df2 = df[['job', 'lt', 'case_number', 'sub_dte', 'ta', 'rev_amt']]
    return df2

def sub_to_cash(d):
    df1 = chks(d)
    df2 = last_mo(d)
    df3 = df1[-df1['lt'].isin(df2['lt'])] #negative sign turns 'isin' to 'NOTisin'
    return df3

def curr_mo(d):
    df = pd.read_csv(r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\PEL\{0}.csv'.format(d['curr_mo_pend']))
    df['rev_amt'] = df['Proc.Fee $'] + df['BOI Fee Amount'] + df['Proc.Fee Amount "DIV"']
    df.rename(columns={'Job#': 'job', 'LT#': 'lt', 'Case Number': 'case_number', 'Process Date': 'sub_dte', 'Trans.Agent Name': 'ta'}, inplace=True)
    df2 = df[['job', 'lt', 'case_number', 'sub_dte', 'ta', 'rev_amt']]
    df3 = df2[df2.sub_dte > int(d['last_mo_pend'].replace('-',''))]
    return df3
    
def full_mo(d):
    df1 = sub_to_cash(d)
    df2 = curr_mo(d)
    df3 = pd.concat([df1,df2], axis=0)
    return df3
    
def lcs_service(conn):
    df = pd.read_sql('''SELECT * FROM lcs_submittals_all''', conn)
    return df

def rev_share_alloc(row):
    if row.ta in ['AT&T/Keane Direct', 'Wells Fargo', 'BROADRIDGE']:
        return 'no'
    elif row.ta == 'American Stock Transfer':
        if row.job in [111991, 115000, 117761, 117815, 117963, 125955, 125700, 140000, 117961]:
            return 'no'
        else:
            return 'yes'
    else:
        return 'yes'

def final_frame(d, conn):
    df1 = full_mo(d)
    df2 = lcs_service(conn)
    df3 = df1.merge(df2[['case_number', 'submittal_type', 'service_provided']], on='case_number', how='left')
    df4 = df3[df3.submittal_type.isnull() == False].copy()
    df4['rev_share'] = df4.apply(rev_share_alloc, axis=1)
    return df4


if __name__ == '__main__':
    main()

