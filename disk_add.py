#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Usage:
  disk_add.py <dbname> <pocet_disku> 
    [--disks_fra|--f <pocet disku do FRA, default=0>] 
    [--output_format|--o <format vystupu choices=('txt', 'json', 'yaml'), default='txt'>]
  disk_add.py -h | --help 

Examples:
    # do db RTOP pridej 3 disky , do FRA nic
    ./disk_add.py RTOP 3
    # 3 disky do DATA, 2 do FRA, vystup ve formatu YAML
    ./disk_add.py TPAUTHP 3 --f 2  --o yaml
    # do db ESPPA pridej 2 disky a 1 do FRA
    ./disk_add.py esppa 2 --f 1

Popis:
    funguje pouze pro VMAX a HITACHI
    disky se pridavaji do datove diskgroup produkce a vsech db na ktere se klonuje
    do FRA se disky defaultne nepridavaji 
        sql zda je treba je zde http://foton.vs.csin.cz/dbawiki/others:fra_pridat_disk
    volitelne parametry jsou pocet disku do FRA a format vystupu
    predpoklada pojmenovani storage group ve formatu <cokoli>_<db>_<D01|DATA|FRA> 
    napr. pr01db_RTOP_DATA
"""
__version__ = '0.1'
__author__ = 'Vasek Polak'



import os
import argparse
import cx_Oracle
import pandas as pd
import subprocess
import shlex
import xml.etree.ElementTree as ET

# Initialize global variables 
SYMCLI_PATH = 'sudo /usr/symcli/bin/'
DG_GROUPS=['D01','DATA','FRA','d01','fra']

# parsovani vstupu
parser = argparse.ArgumentParser(description='zadost o pridani disku do databaze')
parser.add_argument('db',help='jmeno database')
parser.add_argument('disks', type=int,help='pocet disku pro pridani do data diskgroup')
parser.add_argument('--disks_fra','--f', type=int,help='pocet disku pro pridani do fra diskgroup, default=0',default=0)
parser.add_argument('--output_format','--o', help='format vystupu, default=txt',default='txt',choices=('txt', 'json', 'yaml'))

args = parser.parse_args()
DBNAME = args.db.upper()
DISKS=args.disks
DISKS_FRA=args.disks_fra
OUTPUT_FORMAT=args.output_format


# variables
##############
strSqlDbServers="""
-- rac
SELECT AGGREGATE_TARGET_NAME db,
         LISTAGG (replace(host_name,'.vs.csin.cz',''), ',') WITHIN GROUP (ORDER BY AGGREGATE_TARGET_NAME) hosts
    FROM (SELECT AGGREGATE_TARGET_NAME,
                 MEMBER_TARGET_NAME,
                 HOST_NAME,
                 AGGREGATE_TARGET_GUID
            FROM sysman.MGMT$TARGET_MEMBERS m
                 JOIN sysman.MGMT$TARGET t
                     ON m.MEMBER_TARGET_NAME = t.target_name
           WHERE     member_target_type = 'oracle_database'
                 AND AGGREGATE_TARGET_TYPE = 'rac_database')
GROUP BY AGGREGATE_TARGET_NAME, AGGREGATE_TARGET_GUID
UNION
-- single db
SELECT dbname db, replace(host_name,'.vs.csin.cz','') hosts
  FROM sysman.mgmt$target  t
       JOIN (SELECT TARGET_GUID, PROPERTY_VALUE dbname
               FROM sysman.mgmt$target_properties
              WHERE 1 = 1 AND property_name = 'DBName') p
           ON t.target_guid = p.target_guid
 WHERE     1 = 1
       AND TYPE_QUALIFIER3 = 'DB'
       AND target_type = 'oracle_database'
       AND host_name != 'oem12.vs.csin.cz'
"""

sqlCloneConfig="""
select SOURCE_DBNAME,TARGET_DBNAME 
from CLONING_OWNER.CLONING_TARGET_DATABASE 
start with SOURCE_DBNAME ='&SOURCE_DBNAME'
connect by  SOURCE_DBNAME= prior TARGET_DBNAME
"""
sqlCloneConfig = sqlCloneConfig.replace("&SOURCE_DBNAME",DBNAME)




# funkce
#################
def connLocal(dbname):
    """ connect to INFP lokalne jako sysdba """
    os.putenv("LD_LIBRARY_PATH", "/oracle/product/db/12.1.0.2/lib:/opt/rh/python33/root/usr/lib64")
    os.putenv("ORACLE_HOME","/oracle/product/db/12.1.0.2")
    os.putenv("ORACLE_SID",dbname)
    con = cx_Oracle.connect("/", mode = cx_Oracle.SYSDBA)
    return con	

def connWalletSYS(sid,host,port):
    os.putenv("LD_LIBRARY_PATH", "/oracle/product/db/12.1.0.2/lib:/opt/rh/python33/root/usr/lib64")
    os.putenv("ORACLE_HOME","/oracle/product/db/12.1.0.2")
    os.putenv("TNS_ADMIN", "/etc/oracle/wallet/sys")
    dsn = cx_Oracle.makedsn(host, port,service_name = sid)
    try:
        con = cx_Oracle.connect('/', dsn=dsn, mode = cx_Oracle.SYSDBA)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        msg = 'pripojeni do db se nezdarilo -  %s' % (error.message)
        print(msg)
        print(dsn)
        exit(1)
    return con

def makeDictFactory(cursor):
    columnNames = [d[0] for d in cursor.description]
    def createRow(*args):
        return dict(zip(columnNames, args))
    return createRow

def cloneConfig():
    """ vraci dictionary z INFP clone konfigurace """
    conn=connLocal('INFP')
    cur = conn.cursor()
    r = cur.execute(sqlCloneConfig)
    cur.rowfactory = makeDictFactory(cur)
    row = cur.fetchall()
    return row  

def dbnameHost():
    """ vraci dataframe z OMSP, OMST 
              DB                  HOSTS
    0      AFSDA              dpafsdb01
    6       AFSZ    zpr01db01,zpr01db02
    """
    conn=connWalletSYS('OMST','toem.vs.csin.cz','1521')
    cur = conn.cursor()
    r = cur.execute(strSqlDbServers)
    cur.rowfactory = makeDictFactory(cur)
    rows1 = cur.fetchall()
    cur.close()
    conn.close()
    conn=connWalletSYS('OMSP','omsgc.vs.csin.cz','1521')
    cur = conn.cursor()
    r = cur.execute(strSqlDbServers)
    cur.rowfactory = makeDictFactory(cur)
    rows2 = cur.fetchall()
    df=pd.DataFrame(rows1+rows2)
    cur.close()
    conn.close()
    return df 

def listStorageGroupsHITACHI(fieldSid,lDbName):
    """ vraci seznam storage groups z VMAX3 pro jedno pole """
    # sudo raidcom get ldev -ldev_list dp_volume -fx -I998 | grep LDEV_NAMING | grep _DWHP_
    pipe = subprocess.Popen(['sudo','raidcom','get','ldev','-ldev_list','dp_volume','-fx','-I'+str(fieldSid)], universal_newlines=True, stdout=subprocess.PIPE)
    dgList=[line.strip() for line in  pipe.stdout]
    #grep DBNAME pro hitachi storage groups
    sgGroups=[str(fieldSid)+' '+sg.split()[2] for sg in dgList if '_'+lDbName+'_' in sg]
    sgGroups=list(set(sgGroups))
    return sgGroups    

def listStorageGroupsHITACHIall(lDbName):
    """ vraci df napr.
       SID       STORAGEGROUPS    DB
    0  854    pr03db_DWHP_DATA  DWHP
    1  854     pr03db_DWHP_FRA  DWHP
    """
    sgList=[]
    for fieldSid in [854, 998]:
      sgList.extend(listStorageGroupsHITACHI(fieldSid,lDbName))
    xdf=[[item.split()[0],item.split()[1]] for item in sgList]      
    df=pd.DataFrame(xdf, columns=['SID','STORAGEGROUPS'])
    df['DB'] = df['STORAGEGROUPS'].str.split('_',  expand = False).str[-2].str.upper()
    return df

def run_symcli_cmd(symcli_cmd):
  """ 
  :param symcli_cmd: symcli command list parameters with parameters to run
  vraci xml tree
  """
  # prihod prefix na SYMCLI path vcetne volani sudo
  symcli_cmd = os.path.join(SYMCLI_PATH, symcli_cmd.strip())
  args = shlex.split(symcli_cmd.strip())
  try:
    symcli_result = subprocess.run(
        args=args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        universal_newlines=True)
    returncode = symcli_result.returncode
    output = symcli_result.stdout
  except subprocess.CalledProcessError as err:
    logging.exception(err.output)
    raise
  return ET.fromstring(output)

def listSG(sid):
    syminfo_tree = run_symcli_cmd("symsg -sid "+ str(sid) +" list -output xml_e")
    sg_list = [[sid,item.find('name').text] for item in syminfo_tree.findall('SG/SG_Info') 
            if item.find('name').text.split("_")[-1] in DG_GROUPS]
    return sg_list 

def listSGall():
    """
         SID               STORAGEGROUPS        DB
    0    756        dpcesdb01_CESSD_DATA     CESSD
    1    756     dpcpsdb01_CPSTDEVA_DATA  CPSTDEVA
    """
    dfAll=pd.DataFrame(columns=['SID','STORAGEGROUPS'])
    for item in [756, 757, 1441, 1442, 80, 81]:
        x=listSG(item)
        df=pd.DataFrame(x, columns=['SID','STORAGEGROUPS'])
        dfAll=dfAll.append(df)
    dfAll['DB'] = dfAll['STORAGEGROUPS'].str.split('_',  expand = False).str[-2].str.upper()    
    return dfAll   

def dfHitachiDb(db):
    """ vraci df pro jednu db, predpoklada existenci dfDbHost, dfSgHitachi ktere zmerguje a odfiltruje
         DB                HOSTS  SID       STORAGEGROUPS  DISKS
    1  DWHP  pbr03db02,ppr03db01  854    pr03db_DWHP_DATA      2
    2  DWHP  pbr03db02,ppr03db01  998    pr03db_DWHP_DATA      2
    5  DWHP             bporazal  998  bporazal_DWHP_DATA      2
    """
    df = pd.merge(dfDbHost, dfSgHitachi,  on='DB')
    # pokud storage group je orazal(pporazal_esppa_d01) pak server je pporazal
    df.loc[df['STORAGEGROUPS'].str.contains('orazal'), 'HOSTS'] = df['STORAGEGROUPS'].str.split('_').str[0]
    # filtr na db
    df=df[df['DB']==db]
    # pridej sloupec pocet disku
    df['DISKS']=DISKS
    # pro FRA nastav sloupec "DISKS" s hodnotou FRA pocet disku
    df.loc[df['STORAGEGROUPS'].str.contains('FRA|fra'), 'DISKS'] = DISKS_FRA
    # pokud je to source db
    if db == DBNAME :
        df=df[df['DISKS'] > 0]
    else:        
        df=df[~df['STORAGEGROUPS'].str.contains('FRA|fra')]
    return df

def dfHitachiDbAll():
    """ vraci df
         DB                HOSTS  SID       STORAGEGROUPS  DISKS
    0  DWHP  pbr03db02,ppr03db01  854    pr03db_DWHP_DATA      2
    3  DWHP             bporazal  998  bporazal_DWHP_DATA      2
    5  DWHP  pbr03db02,ppr03db01  998    pr03db_DWHP_DATA      2
    """
    dfHitachiAll=dfHitachiDb(DBNAME)
    listCloneDb=cloneConfig()
    for dictCloneDb in listCloneDb:
        df=dfHitachiDb(dictCloneDb['TARGET_DBNAME'])
        dfHitachiAll=pd.concat([dfHitachiAll, df])
    return  dfHitachiAll       

def dfVmaxDb(db):
    """ vraci df pro jednu db, predpoklada existenci dfDbHost, dfSgVmax ktere zmerguje a odfiltruje
           DB                HOSTS   SID       STORAGEGROUPS  DISKS
    475  RTOP             pporazal  1441  pporazal_RTOP_DATA      2
    477  RTOP  pbr01db02,ppr01db01  1441    pr01db_RTOP_DATA      2
    479  RTOP  pbr01db02,ppr01db01  1442    pr01db_RTOP_DATA      2
    """
    dfVMAX = pd.merge(dfDbHost, dfSgVmax,  on='DB')
    # pokud storage group je orazal(pporazal_esppa_d01) pak server je pporazal
    dfVMAX.loc[dfVMAX['STORAGEGROUPS'].str.contains('orazal'), 'HOSTS'] = dfVMAX['STORAGEGROUPS'].str.split('_').str[0]
    # filtr na db
    dfVMAX=dfVMAX[dfVMAX['DB']==db]
    # pridej sloupec pocet disku
    dfVMAX['DISKS']=DISKS
    # pro FRA nastav sloupec "DISKS" s hodnotou FRA pocet disku
    dfVMAX.loc[dfVMAX['STORAGEGROUPS'].str.contains('FRA|fra'), 'DISKS'] = DISKS_FRA
    # pokud je to source db
    if db == DBNAME :
        dfVMAX=dfVMAX[dfVMAX['DISKS'] > 0]
    else:        
        dfVMAX=dfVMAX[~dfVMAX['STORAGEGROUPS'].str.contains('FRA|fra')]
    return dfVMAX

def dfVmaxDbAll():
    """ vraci df
            DB                HOSTS   SID         STORAGEGROUPS  DISKS
    479   RTOP  pbr01db02,ppr01db01  1442      pr01db_RTOP_DATA      2
    206  RTOEA            dprtodb01  1441  dprtodb01_RTOEA_DATA      2
    """
    dfVmaxAll=dfVmaxDb(DBNAME)
    listCloneDb=cloneConfig()
    for dictCloneDb in listCloneDb:
        df=dfVmaxDb(dictCloneDb['TARGET_DBNAME'])
        dfVmaxAll=pd.concat([dfVmaxAll, df])
    return  dfVmaxAll       

def printResult(df):
    if OUTPUT_FORMAT == 'txt':
        df=df.set_index(['DB','SERVER','child_SG'])
        print(df.to_string(justify='right'))
    elif OUTPUT_FORMAT == 'json':
        print(df.to_json(orient='columns'))
    elif OUTPUT_FORMAT == 'yaml':
        import yaml
        print(yaml.dump({'result': df.to_dict(orient='records')},  default_flow_style=False))

# main
############
# vypis vsech db, serveru oddelenych carkou
dfDbHost=dbnameHost()
# kontrola parametru DB zda existuje v produkcnim OEM
if dfDbHost[dfDbHost['DB']==DBNAME].empty:
    print(DBNAME + ' neni v produkcnim OEM')
    exit(1)
# vypis vsech sg 'symsg -sid xxx list'
dfSgVmax=listSGall()
# merge dataframes dfDbHost a dfSgVmax pro vsechny db
dfVmaxDbAll=dfVmaxDbAll()
# sort
dfVmaxDbAll=dfVmaxDbAll.sort_values(['DB','STORAGEGROUPS','SID','HOSTS'])
# prejmenovani sloupcu
dfVmaxDbAll = dfVmaxDbAll.rename(columns={'STORAGEGROUPS': 'child_SG', 'HOSTS': 'SERVER'})
# je to na VMAX? pokud ne zkus HITACHI
if dfVmaxDbAll.empty:
    dfSgHitachi=listStorageGroupsHITACHIall(DBNAME)
    dfHitachiDbAll=dfHitachiDbAll()
    dfHitachiDbAll = dfHitachiDbAll.rename(columns={'STORAGEGROUPS': 'child_SG', 'HOSTS': 'SERVER'})
    if dfHitachiDbAll.empty:
        print('db neni ani na VMAX ani na HITACHI(nebo je sg spatne pojmenovana')
        exit(1)
    printResult(dfHitachiDbAll)
else:    
    printResult(dfVmaxDbAll)
