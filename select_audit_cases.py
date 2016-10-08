from __future__ import print_function
import sqlite3
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

def main():
    db = r'I:\FINANCE\ACCOUNTING\Maranon Reports\2015\New Keane 13-wk Cash\OperatingMetrics\submittals.sqlite'
    conn = sqlite3.connect(db)
    d = dte_rnge()
    sql = sqls(d)
    df = build_out(sql, conn)
    df.to_csv(r'..\{0}_audit_cases.csv'.format(d['month']), index=False)
    conn.close()

def dte_rnge():
    eom = datetime.today() + relativedelta(months=-1, day=31)
    start = eom + relativedelta(days=-5)
    end = eom.strftime('%Y%m%d')
    start = start.strftime('%Y%m%d')
    month = eom.strftime('%Y%m')
    return {'start':start, 'end':end, 'month':month}

def sqls(d):
    morsql = '''SELECT lt, rev_amt, submittal_dte
                FROM mor_submittals
                WHERE submittal_dte BETWEEN {0} AND {1}
                '''.format(d['start'], d['end'])
    
    lcssql = '''SELECT line_of_bus, case_number, rev_amt, submittal_dte
                FROM lcs_submittals_all 
                WHERE submittal_dte BETWEEN {0} AND {1} 
                AND submittal_type = 'LCS Submittal'
                '''.format(d['start'], d['end'])
    
    pelsql = '''SELECT lt, rev_amt, submittal_dte
                FROM pel_submittals
                WHERE submittal_dte BETWEEN {0} AND {1}
                '''.format(d['start'], d['end'])
    return {'mor':morsql, 'pel':pelsql, 'lcs':lcssql}

def build_out(sql, conn):
    p = pd.read_sql(sql['pel'], conn)
    p = p[p.rev_amt > 1000]
    p['source'] = 'PEL'
    m = pd.read_sql(sql['mor'], conn)
    m = m[m.rev_amt > 1000]
    m['source'] = 'MOR'
    l = pd.read_sql(sql['lcs'], conn)
    l = l[(l.rev_amt > 5000) & (l.line_of_bus != 'Banking')]
    l['source'] = 'Location'
    df = pd.concat([p,m,l])
    return df

if __name__ == '__main__':
    main()    