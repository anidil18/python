#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Usage:
  monitor_test_env.py
    [--ignore_db|-i <list db ktere se nemonitoruji>] 
  monitor_test_env.py -h | --help 

Examples:
    # do db RTOP pridej 3 disky , do FRA nic
    ./monitor_test_env.py -i BOSON,CLOUDA
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

# Initialize global variables 
trace_dir = "/var/log/dba"
trace_name = "monitor_test_env.log"
excludeDb=['ovova','TSXO','BOSON','INFTA','CLOUDA','CRMDB']

# parsovani vstupu
parser = argparse.ArgumentParser(description='kontrola testovaciho prostredi')
parser.add_argument('--sysaux_audit_usage_gb','-u', type=int,help='max GB kolik muze zabirat audit v SYSAUX',default=2)
parser.add_argument('--ignore_db','-i', type=str,help='seznam databazi ktere nechci monitorovat napr. -i BOSON,CLOUDA')

args = parser.parse_args()
if args.ignore_db:
    inputIgnoreList = [item for item in args.ignore_db.split(',')]
    excludeDb.extend(inputIgnoreList)
maxAuditGB=args.sysaux_audit_usage_gb

# cx_Oracle variables
os.putenv("LD_LIBRARY_PATH", "/oracle/product/db/12.1.0.2/lib:/opt/rh/python33/root/usr/lib64")
os.putenv("ORACLE_HOME","/oracle/product/db/12.1.0.2")

# variables
###########
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
                  FROM SYSMAN.MGMT$TARGET_PROPERTIES  p1,
                       SYSMAN.MGMT$TARGET_PROPERTIES  p2
                 WHERE     p1.target_type = 'cluster'
                       AND p1.TARGET_guid = p2.TARGET_guid
                       AND p1.property_name = 'scanPort'
                       AND p2.property_name = 'scanName') b
         WHERE a.cluster_name = b.cluster_name) scan
 WHERE d.db = scan.racdb(+)
 """

sqlVdolarOcuppantsAudit="""
select NAME as db, round(SPACE_USAGE_KBYTES/1024/1024) SPACE_USAGE_GB
from V$SYSAUX_OCCUPANTS ,v$database
where occupant_name='AUDSYS'
"""

sqlEnabledArmClientJob="""
select name as db,  ENABLED 
from dba_scheduler_jobs ,v$database
where job_name ='ARM_CLIENT_JOB'
"""

#  functions
############
## logging
# import logging
# import sys
# trace_dir = "/var/log/dba"
def get_log_file():
  tm = time.strftime("%Y%m%d_%H%M%S", time.localtime())
  fn = os.path.join(trace_dir, tm + "_" + trace_name)
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
#   fh1 = logging.StreamHandler(sys.stdout)
#   fh1.setLevel(logging.INFO)
#   fh1.setFormatter(fmt)
#   logging.getLogger().addHandler(fh1)

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
    dsnOMST='(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=toem.vs.csin.cz)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=OMST)(SERVER=DEDICATED)))' 
    con=connDSN('SYS',dsnOMST,passwd)
    cur = con.cursor()
    r = cur.execute(strSqlListDatabases)
    cur.rowfactory = makeDictFactory(cur)
    rows = cur.fetchall()
    df=pd.DataFrame(rows)
    cur.close()
    con.close()
    return df    

def connDSN(username,dsn,passwd):
    try:
        logging.info(dsn)
        if username.upper()=='SYS':
            con = cx_Oracle.connect(username,passwd, dsn=dsn, mode = cx_Oracle.SYSDBA)
        else:
            con = cx_Oracle.connect(username,passwd, dsn=dsn)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        msg = ' %s' % (error.message)
        logging.error(dsn)
        logging.error(msg)
        pass
        return False
    return con

def getPassword():
    cmd = "ansible-vault view /dba/local/ansible/oracle_pass.yml"
    args = shlex.split(cmd.strip())
    try:
        symcli_result = subprocess.run(
            args=args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            universal_newlines=True)
        returncode = symcli_result.returncode
        output = symcli_result.stdout
        password = output.split('\n', 1)[1].split(' ', 1)[1].strip("\n")
    except subprocess.CalledProcessError as err:
        raise
    return password

def printFindings(msg,df):
    delimiter='='*50
    if not df.empty:
        strOutput = msg + '\n' + delimiter + '\n' + df.to_string() + '\n'
    else:
        strOutput = msg + '\n' + delimiter + '\n' + 'zadny problem' + '\n'
    print(strOutput)
    logging.info(strOutput)

def techUctyExpired():
    sql="""
    select target_name from SYSMAN.MGMT$TARGET 
    where target_type in ('oracle_database') 
    and target_name not like '%_on_%' and target_name!='TPTESTA_dbtest107.ack-prg.csint.cz'
    minus
    select target_name from SYSMAN.MGMT$TARGET_METRIC_COLLECTIONS 
    where metric_name ='ME$tech_ucty_expired' 
    """
    dsnOMST='(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=toem.vs.csin.cz)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=OMST)(SERVER=DEDICATED)))' 
    con=connDSN('SYS',dsnOMST,passwd)
    cur = con.cursor()
    r = cur.execute(sql)
    cur.rowfactory = makeDictFactory(cur)
    rows = cur.fetchall()
    df=pd.DataFrame(rows)
    cur.close()
    con.close()
    return df    

#
# Main
##############
# passwd=getPassword()
passwd=(base64.b64decode('UCNzdDZyZXFQI3N0NnJlcQ=='))

df=listOEMdbOMST()
df=df.drop_duplicates(subset='TNS')

# ignoruj seznam db
df=df[~df['DB'].isin(excludeDb)]

## vyber db
# df=df.head(30)
# df=df[df['DB'].isin(excludeDb)]
# print(df)

rowAllAudit=[]
rowAllEnabled=[]
for index, rowDF in df.iterrows():    
    try:
        con=connDSN('dbsnmp',rowDF['TNS'],passwd)
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
# tech ucty expired apply
dfTechUctyExpired=techUctyExpired()


# aplikuj podminky
dfBig=dfAudit[dfAudit['SPACE_USAGE_GB']>maxAuditGB]
dfEnabled=dfEnabled[dfEnabled['ENABLED']=='FALSE']

# tiskni vysledky
printFindings('db kde audit zabira v sysaux vice jak ' + str(maxAuditGB) + 'GB',dfBig)
printFindings('db kde neni enabled arm_client_job',dfEnabled)
printFindings('db kde neni aplikovana metrika ME$tech_ucty_expired',dfTechUctyExpired)
