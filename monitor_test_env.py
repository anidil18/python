#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Usage:
  monitor_test_env.py
  monitor_test_env.py -h | --help 
"""
__version__ = '0.1'
__author__ = 'Vasek Polak'

import argparse
import datetime
import logging
import os
import shlex
import subprocess
import sys
import time
from time import gmtime, strftime
import base64

import cx_Oracle
import numpy as np
import pandas as pd

# Konstanty
TRACE_DIR = "/var/log/dba"
TRACE_NAME = "monitor_test_env.log"
EXCLUDE_DB=['ovova','TSXO','BOSON','INFTA','CLOUDA','CRMDB']

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', -1)

# globalni promenne
resultsFindings=False
resultsMonitored=""

# parsovani vstupu
parser = argparse.ArgumentParser(description='kontrola testovaciho prostredi')
parser.add_argument('--min_tbs_gb','-m', type=int,help='minimu GB volneho mista v systemovych tablespaces',default=3)
parser.add_argument('--hours_not_collected','-o', type=int,help='cas v hodinach do kdy se maji sbirat metriky',default=24)
parser.add_argument('--sysaux_audit_usage_gb','-u', type=int,help='max GB kolik muze zabirat audit v SYSAUX',default=5)
parser.add_argument('--ignore_db','-i', type=str,help='seznam databazi ktere nechci monitorovat napr. -i BOSON,CLOUDA')

args = parser.parse_args()
if args.ignore_db:
    inputIgnoreList = [item for item in args.ignore_db.split(',')]
    EXCLUDE_DB.extend(inputIgnoreList)
maxAuditGB=args.sysaux_audit_usage_gb
minTbsGb=str(args.min_tbs_gb)
hoursNotCollected=str(args.hours_not_collected)

# cx_Oracle variables
os.putenv("LD_LIBRARY_PATH", "/oracle/product/db/12.1.0.2/lib:/opt/rh/python33/root/usr/lib64")
os.putenv("ORACLE_HOME","/oracle/product/db/12.1.0.2")
os.putenv("TNS_ADMIN", '/etc/oracle/wallet/system')

# variables
###########
sqlTruncateAudit="""
DECLARE
    l_audsize    NUMBER := 2;
    v_aud_size   NUMBER;
    v_dbname     VARCHAR (200);
BEGIN
    SELECT TRUNC (SPACE_USAGE_KBYTES / 1024 / 1024) GB
      INTO v_aud_size
      FROM V$SYSAUX_OCCUPANTS
     WHERE occupant_name = 'AUDSYS';

    SELECT name INTO v_dbname FROM v$database;

    DBMS_OUTPUT.PUT_LINE (
           v_dbname
        || ': size of AUDSYS in V$SYSAUX_OCCUPANTS: '
        || v_aud_size
        || 'GB');

    IF v_aud_size >= 2
    THEN
        DBMS_AUDIT_MGMT.clean_audit_trail (
            audit_trail_type          => DBMS_AUDIT_MGMT.audit_trail_unified,
            use_last_arch_timestamp   => FALSE);

        SELECT TRUNC (SPACE_USAGE_KBYTES / 1024 / 1024) GB
          INTO v_aud_size
          FROM V$SYSAUX_OCCUPANTS
         WHERE occupant_name = 'AUDSYS';

        DBMS_OUTPUT.PUT_LINE (
               v_dbname
            || ': size of AUDSYS in V$SYSAUX_OCCUPANTS after truncate: '
            || v_aud_size
            || 'GB');
    END IF;
END;
"""

strSqlListDatabases="""
SELECT HOST,
       DB,
       PORT,
       INSTANCE,
       LIFECYCLE,
       RAC,
       OS,
       UP,
       PROD,
       SERVICE,
       CLUSTER_NAME,
       SCAN_NAME,
       SCAN_PORT,
       RACDB,
          '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST='
       || DECODE (scan_name, '', HOST, scan_name)
       || ')(PORT='
       || port
       || ')))(CONNECT_DATA=(SERVICE_NAME='
       || service
       || ')(SERVER=DEDICATED)))'
           TNS
  FROM (SELECT HOST.PROPERTY_VALUE
                   HOST,
               dbname.PROPERTY_VALUE
                   db,
               port.PROPERTY_VALUE
                   port,
               sid.PROPERTY_VALUE
                   instance,
               lf.PROPERTY_VALUE
                   lifecycle,
               rac.PROPERTY_VALUE
                   rac,
               os.PROPERTY_VALUE
                   os,
               UP,
               DECODE (lf.PROPERTY_VALUE, 'Production', 'Y', 'N')
                   Prod,
               DECODE (domain,
                       '', dbname.PROPERTY_VALUE,
                       dbname.PROPERTY_VALUE || domain)
                   service
          FROM sysman.mgmt_targets  tn,
               (SELECT target_guid, property_value
                  FROM sysman.mgmt_target_properties
                 WHERE property_name = 'MachineName') HOST,
               (SELECT target_guid, property_value
                  FROM sysman.mgmt_target_properties
                 WHERE property_name = 'Port') port,
               (SELECT target_guid, property_value
                  FROM sysman.mgmt_target_properties
                 WHERE property_name = 'SID') sid,
               (SELECT target_guid, property_value
                  FROM sysman.mgmt_target_properties
                 WHERE property_name = 'DBName') DBNAME,
               (SELECT target_guid, property_value
                  FROM sysman.MGMT$TARGET_PROPERTIES
                 WHERE property_name = 'orcl_gtp_lifecycle_status') lf,
               (SELECT target_guid, property_value
                  FROM sysman.MGMT$TARGET_PROPERTIES
                 WHERE property_name = 'RACOption') rac,
               (SELECT target_guid, property_value
                  FROM sysman.MGMT$TARGET_PROPERTIES
                 WHERE property_name = 'orcl_gtp_os') os,
               (SELECT target_guid,
                       CASE AVAILABILITY_STATUS
                           WHEN 'Target Up' THEN 'Y'
                           ELSE 'N'
                       END
                           UP
                  FROM sysman.MGMT$AVAILABILITY_CURRENT) UP,
               (SELECT TARGET_GUID, '.' || PROPERTY_VALUE AS domain
                  FROM sysman.MGMT$TARGET_PROPERTIES
                 WHERE     property_name = 'DBDomain'
                       AND target_type = 'oracle_database'
                       AND PROPERTY_VALUE IS NOT NULL) domain
         WHERE     tn.target_guid = HOST.target_guid
               AND tn.target_guid = port.target_guid
               AND tn.target_guid = sid.target_guid
               AND tn.target_guid = lf.target_guid
               AND tn.target_guid = dbname.target_guid
               AND tn.target_guid = rac.target_guid
               AND tn.target_guid = os.target_guid
               AND tn.target_guid = UP.target_guid
               AND tn.target_type = 'oracle_database'
               AND tn.target_guid = domain.target_guid(+)) d,
       (SELECT a.cluster_name,
               b.scan_name,
               scan_port,
               racdb
          FROM (SELECT SOURCE_TARGET_NAME racdb,
                       ASSOC_TARGET_NAME  cluster_name
                  FROM sysman.MGMT$TARGET_ASSOCIATIONS
                 WHERE     assoc_def_name = 'runs_on_cluster'
                       AND source_target_type = 'rac_database') a,
               (SELECT p1.TARGET_NAME    cluster_name,
                       p2.PROPERTY_VALUE scan_name,
                       p1.PROPERTY_VALUE scan_port
                  FROM sysman.MGMT$TARGET_PROPERTIES  p1,
                       sysman.MGMT$TARGET_PROPERTIES  p2
                 WHERE     p1.target_type = 'cluster'
                       AND p1.TARGET_guid = p2.TARGET_guid
                       AND p1.property_name = 'scanPort'
                       AND p2.property_name = 'scanName') b
         WHERE a.cluster_name = b.cluster_name) scan
 WHERE d.db = scan.racdb(+)
"""

sqlVdolarOcuppantsAudit="""
select NAME as db, round(SPACE_USAGE_KBYTES/1024/1024) SPACE_USAGE_GB,
'ssh torsmdb2.vs.csin.cz /dba/local/bin/ARM/reinstall_arm_client.sh '|| name as reinstall_from_oem
from V$SYSAUX_OCCUPANTS ,v$database
where occupant_name='AUDSYS'
"""

sqlEnabledArmClientJob="""
select name as db,  ENABLED ,'ssh torsmdb2.vs.csin.cz /dba/local/bin/ARM/recreate_arm_client_job.sh '||name as run_from_oem
from dba_scheduler_jobs ,v$database
where job_name ='ARM_CLIENT_JOB'
"""

sqlOemMetricNotCollected="""
select distinct TARGET_NAME, metric_name,METRIC_LABEL
,COLLECTION_TIMESTAMP last_COLLECTION
from SYSMAN.MGMT$METRIC_CURRENT
where 1=1
and metric_name in ('problemTbsp','DiskGroup_Usage')
and target_guid not in (select TARGET_GUID from SYSMAN.MGMT$BLACKOUT_HISTORY where status='Started')
and COLLECTION_TIMESTAMP < sysdate - interval '&hoursNotCollected' hour
order by COLLECTION_TIMESTAMP
"""
sqlOemMetricNotCollected = sqlOemMetricNotCollected.replace("&hoursNotCollected", hoursNotCollected)

sqlTechUctyExpired="""
select target_name from SYSMAN.MGMT$TARGET
where target_type in ('oracle_database') 
and target_name not like '%_on_%' and target_name!='TPTESTA_dbtest107.ack-prg.csint.cz'
minus
select target_name from SYSMAN.MGMT$TARGET_METRIC_COLLECTIONS 
where metric_name ='ME$tech_ucty_expired' 
"""

sqlTbsFreeSpace="""
select TARGET_NAME db, KEY_VALUE tablespace, round(to_number(VALUE)/1024) free_GB,COLLECTION_TIMESTAMP last_COLLECTION
from SYSMAN.MGMT$METRIC_CURRENT
where 1=1
and metric_name='problemTbsp'
and metric_column='bytesFree'
and key_value in ('SYSAUX','SYSTEM','ARM_DATA','ARM_ADM_DATA','ARM_SRV_DATA','ARM_SRV_IDX','ARM_SRV_LOB','ARM_SRV_STAGE')
and target_guid not in (select TARGET_GUID from SYSMAN.MGMT$BLACKOUT_HISTORY where status='Started')
and target_name not like '%_on_%'
and round(to_number(VALUE)/1024) < &minTbsGb
and not REGEXP_LIKE (target_name, '^S(D|K)..$')
order by round(to_number(VALUE)/1024)
"""
sqlTbsFreeSpace=sqlTbsFreeSpace.replace("&minTbsGb",minTbsGb)

#  functions
############
## logging
def get_log_file():
  tm = time.strftime("%Y%m%d_%H%M%S", time.localtime())
  fn = os.path.join(TRACE_DIR, tm + "_" + TRACE_NAME)
  return fn

def redirect_stderr(filename):
  fh = open(filename, "a")
  sys.stderr = fh

def set_logging():
  logging.basicConfig(
      format="%(asctime)s %(levelname)s %(message)s",
      datefmt="%Y-%m-%d %X",
      level=logging.DEBUG,
      stream=sys.stderr)
  fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %X")

def init():
  log_file = get_log_file()
  redirect_stderr(log_file)
  set_logging()

init()
## konec logging

def makeDictFactory(cursor):
    columnNames = [d[0] for d in cursor.description]
    def createRow(*args):
        return dict(zip(columnNames, args))
    return createRow

def listOEMdbOMST():
    """ 
    CLUSTER_NAME            zr01db-cluster
    DB                               COLRZ
    HOST              zpr01db02.vs.csin.cz
    INSTANCE                        COLRZ2
    LIFECYCLE               Pre-production
    OS                               Linux
    PORT                              1521
    PROD                                 N
    RAC                                YES
    RACDB                            COLRZ
    SCAN_NAME       zr01db-scan.vs.csin.cz
    SCAN_PORT                         1521
    SERVICE               COLRZ.vs.csin.cz
    TNS           (DESCRIPTION=(ADDRESS...
    UP                                   Y
    TNS    (DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=zr01db-scan.vs.csin.cz)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=COLRZ.vs.csin.cz)(SERVER=DEDICATED)))
    """ 
    con=connDashboard('OMST')
    cur = con.cursor()
    r = cur.execute(strSqlListDatabases)
    cur.rowfactory = makeDictFactory(cur)
    rows = cur.fetchall()
    df=pd.DataFrame(rows)
    cur.close()
    con.close()
    return df    

def connDashboard(omsdb):
    DASHBOARD_PASSWD=(base64.b64decode('YWJjZDEyMzQ='))
    try:
        logging.info(omsdb)
        if omsdb=='OMSP':
            dsn='(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=omsgc.vs.csin.cz)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=OMSP)(SERVER=DEDICATED)))' 
        else:
            dsn='(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=toem.vs.csin.cz)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=OMST)(SERVER=DEDICATED)))' 
        con = cx_Oracle.connect('DASHBOARD',DASHBOARD_PASSWD, dsn=dsn)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        msg = ' %s' % (error.message)
        logging.error(omsdb)
        logging.error(msg)
        return False
    return con

def conWallet(dsn):
    try:
        logging.info(dsn)
        con = cx_Oracle.connect('/', dsn=dsn)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        msg = ' %s' % (error.message)
        logging.error(dsn)
        logging.error(msg)
        pass
        return False
    return con 

def printFindings(msg,df):
    global resultsFindings , resultsMonitored
    delimiter='='*50
    if not df.empty:
        strOutput = msg + '\n' + delimiter + '\n' + df.to_string() + '\n'
        resultsFindings=True
        print(strOutput)
        logging.info(strOutput)
    resultsMonitored=resultsMonitored + '\n' + msg
    return True

def printSumary():
    delimiterBig='='*80
    delimiterSmall='-'*30
    headerSmall='\n' + "probehly tyto kontroly"
    allSummary=delimiterBig + '\n' + headerSmall + '\n' + delimiterSmall +  resultsMonitored
    logging.info(allSummary)
    print(allSummary)

def noFindings():
    noFindingsText='\n' + "Vsechny kontroly probehly v poradku" + '\n'
    logging.info(noFindingsText)
    print(noFindingsText)

def listDb():
    dfOEMdbOMST=listOEMdbOMST()
    dfOEMdbOMST=dfOEMdbOMST.drop_duplicates(subset='TNS')
    # ignoruj seznam db
    dfOEMdbOMST=dfOEMdbOMST[~dfOEMdbOMST['DB'].isin(EXCLUDE_DB)]
    ## vyber db
    # dfOEMdbOMST=dfOEMdbOMST.head(3)
    # dfOEMdbOMST=dfOEMdbOMST[dfOEMdbOMST['DB'].isin(['DWHTA3'])]
    # print(dfOEMdbOMST)
    return dfOEMdbOMST

def doTruncateAudit():
    df=listDb()
    for index, rowDF in df.iterrows(): 
        # print(rowDF['TNS'])   
        try:
            con=conWallet(rowDF['TNS'])
            cur = con.cursor()
            cur.callproc("dbms_output.enable", (None,))
            cur.execute("ALTER SESSION SET ddl_lock_timeout = 300")
            cur.execute(sqlTruncateAudit)
            statusVar = cur.var(cx_Oracle.NUMBER)
            lineVar = cur.var(cx_Oracle.STRING)
            while True:
                cur.callproc("dbms_output.get_line", (lineVar, statusVar))
                if statusVar.getvalue() != 0:
                    break
                # print(lineVar.getvalue())
                logging.info(lineVar.getvalue())
            cur.close()
            con.close()
        except:
            pass  

def doEachDb():
    df=listDb()
    rowAllAudit=[]
    rowAllEnabled=[]
    for index, rowDF in df.iterrows():    
        try:
            con=conWallet(rowDF['TNS'])
            # audit
            cur = con.cursor()
            r = cur.execute(sqlVdolarOcuppantsAudit)
            cur.rowfactory = makeDictFactory(cur)
            row = cur.fetchall()
            rowAllAudit += row
            cur.close()
            # enabled arm job
            cur = con.cursor()
            r = cur.execute(sqlEnabledArmClientJob)
            cur.rowfactory = makeDictFactory(cur)
            row = cur.fetchall()
            rowAllEnabled += row
            cur.close()
        except:
            pass
    # vytvor dataframe         
    dfAudit=pd.DataFrame(rowAllAudit)
    dfEnabled=pd.DataFrame(rowAllEnabled)
    dfAudit=dfAudit.set_index('DB')
    dfEnabled=dfEnabled.set_index('DB')
    # aplikuj podminky
    dfBig=dfAudit[dfAudit['SPACE_USAGE_GB']>maxAuditGB]
    dfEnabled=dfEnabled[dfEnabled['ENABLED']=='FALSE']
    # prehod poradi sloupcu
    dfBig = dfBig[['SPACE_USAGE_GB', 'REINSTALL_FROM_OEM']]
    # tiskni vysledky
    printFindings('db kde audit zabira v sysaux vice jak ' + str(maxAuditGB) + 'GB',dfBig)
    printFindings('db kde neni enabled arm_client_job',dfEnabled)

def techUctyExpired():
    dsn='(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=toem.vs.csin.cz)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=OMST)(SERVER=DEDICATED)))' 
    con=connDashboard('OMST')
    cur = con.cursor()
    r = cur.execute(sqlTechUctyExpired)
    cur.rowfactory = makeDictFactory(cur)
    rows = cur.fetchall()
    df=pd.DataFrame(rows)
    cur.close()
    con.close()
    return df 

def doTechUctyExpired():
    # tech ucty expired apply
    dfTechUctyExpired=techUctyExpired()
    # tiskni vysledky
    printFindings('db kde neni aplikovana metrika ME$tech_ucty_expired',dfTechUctyExpired)

def OemMetricNotCollected():
    dsn='(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=toem.vs.csin.cz)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=OMST)(SERVER=DEDICATED)))' 
    con=connDashboard('OMST')
    cur = con.cursor()
    r = cur.execute(sqlOemMetricNotCollected)
    cur.rowfactory = makeDictFactory(cur)
    rows = cur.fetchall()
    df=pd.DataFrame(rows)
    cur.close()
    con.close()
    return df 

def doOemMetricNotCollected():
    # tech ucty expired apply
    dfOemMetricNotCollected=OemMetricNotCollected()
    # tiskni vysledky
    printFindings('db,asm kde se nesbiraji metriky(a target neni v blackoutu) starsi nez '+hoursNotCollected+'hodin',dfOemMetricNotCollected)


def TbsFreeSpace():
    dsn='(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=toem.vs.csin.cz)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=OMST)(SERVER=DEDICATED)))' 
    con=connDashboard('OMST')
    cur = con.cursor()
    r = cur.execute(sqlTbsFreeSpace)
    cur.rowfactory = makeDictFactory(cur)
    rows = cur.fetchall()
    df=pd.DataFrame(rows)
    cur.close()
    con.close()
    if not df.empty:
        df=df.set_index('DB')
    return df 

def doTbsFreeSpace():
    # tech ucty expired apply
    dfTbsFreeSpace=TbsFreeSpace()
    # tiskni vysledky
    printFindings('tbs kde je min mista nez ' + minTbsGb + 'G (a target neni v blackoutu) - SYSAUX,SYSTEM,ARM_DATA',dfTbsFreeSpace)

#
# Main
##############
# jednotlive kontroly(pokud je nalez tak se vytiskne)
doTruncateAudit()
doEachDb()
# doTechUctyExpired()
# doOemMetricNotCollected()
doTbsFreeSpace()

# tiskni zahlavi a co se kontrolovalo
if not resultsFindings: noFindings()
printSumary()