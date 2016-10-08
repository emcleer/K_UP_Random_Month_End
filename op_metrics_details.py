from __future__ import print_function
import sqlite3
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta, FR, MO

def main():
    db = r'I:\FINANCE\ACCOUNTING\Maranon Reports\2015\New Keane 13-wk Cash\OperatingMetrics\submittals.sqlite'
    conn = sqlite3.connect(db)
    
    #dtes = dte_rnge()
    dtes = {'start': '20160601', 'end': '20160630'}
                                
    morsql = '''SELECT l.line_of_bus, m.lt, m.rev_amt, m.submittal_dte, m.job, m.ta
                FROM mor_submittals as m
                LEFT OUTER JOIN lcs_submittals_all as l
                ON m.case_number = l.case_number 
                WHERE m.submittal_dte BETWEEN {0} AND {1}
                '''.format(dtes['start'], dtes['end'])
    
    lcssql = '''SELECT line_of_bus, case_number, rev_amt, submittal_dte
                FROM lcs_submittals_all 
                WHERE submittal_dte BETWEEN {0} AND {1} 
                AND submittal_type = 'LCS Submittal'
                '''.format(dtes['start'], dtes['end'])
    
    pelsql = '''SELECT l.line_of_bus, l.service_provided, p.lt, p.rev_amt, p.submittal_dte, p.job, p.ta
                FROM pel_submittals as p
                LEFT OUTER JOIN lcs_submittals_all as l
                ON p.case_number = l.case_number 
                WHERE p.submittal_dte BETWEEN {0} AND {1}
                '''.format(dtes['start'], dtes['end'])
    
    #dupsql = '''SELECT COALESCE(line_of_bus, source) AS lob, COUNT(COALESCE(case_number_keastone, lt)), SUM(rev_amt)
    #            FROM duplicate_submittals
    #            WHERE submittal_dte BETWEEN {0} AND {1}
    #            GROUP BY lob '''.format(dtes['start'], dtes['end'])
    
            
    total_company(morsql, lcssql, pelsql, conn, dtes).to_csv(r'..\{0}_op_metrics_details.csv'.format(dtes['end'][:6]), index=False)
    
    #dup_submittals(dupsql, conn, dtes)
        
def dte_rnge():  
    l_fri = datetime.today() + relativedelta(weekday=FR(-1))
    l_mon = l_fri + relativedelta(weekday=MO(-1))
    l_fri_string = l_fri.strftime('%Y%m%d')   #no dashes for SQLite query
    l_mon_string = l_mon.strftime('%Y%m%d')   #no dashes for SQLite query
    return {'start': l_mon_string, 'end': l_fri_string}

def pel_alloc(row):
    if (row['line_of_bus'] == 'Securities') and ((row['service_provided'] == 'DRO') or (row['service_provided'] == 'Deep Search')):
        return 'Bulk Sec Add-back'
    
    elif (row['line_of_bus'] == 'International') and ((row['service_provided'] == 'DRO') or (row['service_provided'] == 'Deep Search')):
        return 'Bulk Int\'l Add-back'
    
    else:
        return 'PEL'
      
def pel(sql, conn, d):
    df = pd.read_sql(sql, conn)
    df.line_of_bus = df.apply(pel_alloc, axis=1)
    df2 = df[['line_of_bus', 'lt', 'rev_amt', 'submittal_dte', 'job', 'ta']].copy()
    #grp = df2.groupby('line_of_bus').sum()
    #df3 = pd.DataFrame(grp).reset_index()
    df2.rename(columns={'lt':'case_lt', 'rev_amt':'revenue'}, inplace=True)
    return df2

def mor(sql, conn, d):
    df = pd.read_sql(sql, conn)
    df.line_of_bus = 'MOR'
    #grp = df.groupby('line_of_bus').sum()
    #df2 = pd.DataFrame(grp).reset_index()
    df.rename(columns={'lt':'case_lt', 'rev_amt':'revenue'}, inplace=True)
    return df

def lcs(sql, conn, d):
    df = pd.read_sql(sql, conn)
    df.rename(columns={'case_number':'case_lt', 'rev_amt':'revenue'}, inplace=True)
    return df

def total_company(sql1, sql2, sql3, conn, d):
    dfs = []
    dfs.append(mor(sql1, conn, d))
    dfs.append(lcs(sql2, conn, d))
    dfs.append(pel(sql3, conn, d))
    total = pd.concat(dfs, axis=0).reset_index(drop=True)
    total.revenue = total.revenue.apply(lambda x: '{:,}'.format(x))
    return total

def dup_submittals(sql, conn, d):
    df = pd.read_sql(sql, conn)
    df.rename(columns={'lob':'line_of_bus', 'COUNT(COALESCE(case_number_keastone, lt))':'count', 'SUM(rev_amt)': 'revenue'}, inplace=True)
    df.revenue = df.revenue.apply(lambda x: '{:,}'.format(x))
    print (df)
         
if __name__ == '__main__':
    main()
