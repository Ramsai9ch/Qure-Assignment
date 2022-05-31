import pandas as pd
import mysql.connector
import requests
import sys
import getopt
import time

def add_string(s,s1):
    s=s+'\n'+s1
    return s

def result(cur,month):
    month_dict={3:"March",4:"April",5:"May",6:"June"}
    s="Top 3 counties with highest number of covid deaths from top3 states(by covid deaths) for month of %s"%(month_dict[month])
    month_="\nMonth-%s"%(month_dict[month])
    s=add_string(s,month_)
    cur.execute("select fips,sum(deaths) from stateleveldata where MONTH(date) = %s group by fips order by sum(deaths) desc limit 3"%month)
    fips_highest=cur.fetchall()
    for i in fips_highest:
        fip=i[0]
        cur.execute("select state from stateleveldata where fips=%s limit 1"%fip)
        fip_cases="\n"+cur.fetchall()[0][0]+"--"+str(int(i[1]))
        s=add_string(s,fip_cases)
        cur.execute("""select county_fips,min(state_fips),sum(deaths) from countyleveldata where MONTH(date) = 4 group by county_fips HAVING min(state_fips)= %s order by sum(deaths) desc limit 3"""%fip)
        county_output=cur.fetchall()
        for i in county_output:
            county_fip=i[0]
            cur.execute("select county_name from countyleveldata where county_fips=%s limit 1"%county_fip)
            county_cases=cur.fetchall()[0][0]+"--"+str(int(i[2]))
            s=add_string(s,county_cases)
    return s

def send_slack_messages(message):
    payload= '{"text" : "%s"}' %(message)
    response=requests.post('https://hooks.slack.com/services/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',data=payload)
    return response




conn=mysql.connector.connect(host='localhost',user="root",password="Ramsai@sql",database="qure")
cur=conn.cursor()
months=[3,4,5,6]
for i in months:
    s=result(cur,i)
    print(send_slack_messages(s))
    time.sleep(20)
    
