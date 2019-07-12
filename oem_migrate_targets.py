#!/usr/bin/env python3

import subprocess
import base64
import os
import cx_Oracle
import argparse

# arg parsing
#############
parser = argparse.ArgumentParser(
    description='parametry pro presun targetu z omsp na omst',
    epilog="""
    ./oem_migrate_targets.py --server=zpr01db02.vs.csin.cz \ --RacServerName1=zpr01db01.vs.csin.cz --RacServerName2=zpr01db02.vs.csin.cz \ --RAC=True
    """)
parser.add_argument('--server',default='xxx',help='targety na serveru')
parser.add_argument('--RacServerName1',default='xxx',help='RAC server cislo 1')
parser.add_argument('--RacServerName2',default='xxx',help='RAC server cislo 2')
parser.add_argument('--RAC',default=False,help='je to RAC?')
args = parser.parse_args()
server=args.server
RacServerName1=args.RacServerName1
RacServerName2=args.RacServerName2
RAC=args.RAC
print('server={} RacServerName1={} RacServerName2={} RAC={}'.format(server,RacServerName1,RacServerName2,RAC))

# variables
###########
# server='zpr01db02.vs.csin.cz'
# RacServerName1='zpr01db01.vs.csin.cz'
# RacServerName2='zpr01db02.vs.csin.cz'
sqlAddCluster="""
select
'emcli add_target -monitor_mode=1 -type=cluster -host='
||host_name||' -name='||target_name
||' -instances="&RacServerName1:host;&RacServerName2:host" -properties='
||'"'
||'OracleHome:'||OracleHome
||';eonsPort:'||eonsPort
||';scanName:'||scanName
||';scanPort:'||scanPort
||';orcl_gtp_lifecycle_status:'||orcl_gtp_lifecycle_status
||'"'
as emclicmd
from
(
select HOST_NAME,t.TARGET_NAME, t.TARGET_TYPE,  PROPERTY_NAME, PROPERTY_VALUE
from sol60237.target_190305 t, sol60237.target_properties_190305 p
where t.target_guid=p.target_guid
and host_name in ('&RacServerName1','&RacServerName2')
and t.target_type='cluster'
and property_name in ('OracleHome','eonsPort','scanName','scanPort','orcl_gtp_lifecycle_status')
)
PIVOT (MAX (property_VALUE) FOR property_name in
('OracleHome' OracleHome,'eonsPort' eonsPort,'scanName' scanName,'scanPort' scanPort,'orcl_gtp_lifecycle_status' orcl_gtp_lifecycle_status))
"""


sqlAddDatabaseInstance="""
SELECT    'emcli add_target -name='
       || target_name
       || ' -type=oracle_database -host='
       || host_name
       || ' -properties='
       || '"'
       || 'SID:'
       || SID
       || ';MachineName:'
       || MachineName
       || ';OracleHome:'
       || OracleHome
       || ';Port:'
       || Port
       || ';orcl_gtp_lifecycle_status:'
       || lifecycle
       || '"'
       || ' -credentials="UserName:dbsnmp;password:abcd1234;Role:normal"'
           AS cmd
  FROM (SELECT *
          FROM (SELECT p.target_name,
                       target_type,
                       property_name,
                       property_value,
                       host_name,
                       -- lifecycle preber z serveru, pokud tam chybi, pak Test
                       nvl(lifecycle,'Test') lifecycle
                  FROM SOL60237.TARGET_PROPERTIES_190305  p,
                       (SELECT target_name,
                               host_name,
                               (SELECT property_value
                                  FROM SOL60237.TARGET_PROPERTIES_190305
                                 WHERE     target_name =
                                           '&server'
                                       AND property_name =
                                           'orcl_gtp_lifecycle_status')
                                   AS lifecycle
                          FROM SOL60237.TARGET_190305
                         WHERE host_name = '&server') t
                 WHERE     p.TARGET_NAME = t.target_name
                       AND property_name IN ('MachineName',
                                             'OracleHome',
                                             'Port',
                                             --'orcl_gtp_lifecycle_status',
                                             'target_type',
                                             'SID'))
               PIVOT (MAX (property_value)
                     FOR property_name
                     IN ('MachineName' AS MachineName,
                        'OracleHome' AS OracleHome,
                        'Port' AS Port,
                        --'orcl_gtp_lifecycle_status' AS orcl_gtp_lifecycle_status,
                        'SID' AS sid))
         WHERE 1 = 1 AND target_type = 'oracle_database')
"""

sqlAddRacDatabase="""
SELECT    'emcli add_target -name='
       || target_name
       || ' -monitor_mode=1 -type=rac_database -host='
       || host_name
       || ' -properties='
       || '"'
       || 'ServiceName:'
       || ServiceName
       || ';ClusterName:'
       || ClusterName
       || ';orcl_gtp_lifecycle_status:'
       || orcl_gtp_lifecycle_status
       || '"'
       || ' -instances="'
       || dbname
       || '_'
       || dbname
       || '1:oracle_database;'
       || dbname
       || '_'
       || dbname
       || '2:oracle_database"'
           AS cmd
  FROM (SELECT *
          FROM (SELECT p.target_name,
                       target_type,
                       property_name,
                       property_value,
                       host_name,
                       lifecycle
                  FROM SOL60237.TARGET_PROPERTIES_190305  p,
                       (SELECT target_name,
                               host_name,
                               (SELECT property_value
                                  FROM SOL60237.TARGET_PROPERTIES_190305
                                 WHERE     target_name =
                                           '&server'
                                       AND property_name =
                                           'orcl_gtp_lifecycle_status')
                                   AS lifecycle
                          FROM SOL60237.TARGET_190305
                         WHERE host_name = '&server') t
                 WHERE     p.TARGET_NAME = t.target_name
                 and property_name IN ('MachineName',
                                         'ServiceName',
                                         'orcl_gtp_lifecycle_status',
                                         'target_type',
                                         'ClusterName',
                                         'DBName'))
               PIVOT (MAX (property_value)
                     FOR property_name
                     IN ('MachineName' AS MachineName,
                        'ServiceName' AS ServiceName,
                        'orcl_gtp_lifecycle_status' AS orcl_gtp_lifecycle_status,
                        'ClusterName' ClusterName,
                        'DBName' DBName))
         WHERE     1 = 1
               AND target_type = 'rac_database'
               )
"""

addOsmInstances="""
SELECT    'emcli add_target -name='
       || target_name
       || ' -type=osm_instance -host='
       || host_name
       || ' -credentials="UserName:asmsnmp;password:abcd1234;Role:normal"'
       || ' -properties='
       || '"'
       || 'SID:'
       || SID
       || ';MachineName:'
       || MachineName
       || ';OracleHome:'
       || OracleHome
       || ';Port:1521'
       || '"'
  FROM (SELECT t.TARGET_NAME,
               t.TARGET_TYPE,
               t.host_name,
               PROPERTY_NAME,
               PROPERTY_VALUE
          FROM SOL60237.TARGET_190305  t
               JOIN SOL60237.TARGET_PROPERTIES_190305 p
                   ON t.target_guid = p.target_guid
         WHERE     host_name = '&server'
               AND t.target_type = 'osm_instance'
               AND property_name IN ('SID',
                                     'MachineName',
                                     'OracleHome',
                                     'orcl_gtp_lifecycle_status'))
       PIVOT (MAX (property_value)
             FOR property_name
             IN ('SID' AS sid,
                'MachineName' AS MachineName,
                'OracleHome' AS OracleHome,
                'orcl_gtp_lifecycle_status' AS orcl_gtp_lifecycle_status))
"""

addOsmCluster="""
SELECT    'emcli add_target -name='
       || target_name
       || '  -monitor_mode=1 -type=osm_cluster -host='
       || host_name
       || ' -properties='
       || '"'
       || 'ServiceName:'
       || ServiceName
       || ';ClusterName:'
       || ClusterName
       || ';orcl_gtp_lifecycle_status:'
       || orcl_gtp_lifecycle_status
       || '"'
       || ' -instances='
       || '"'
       || '+ASM1_&RacServerName1:osm_instance;+ASM2_&RacServerName2:osm_instance'
       || '"'
  FROM (SELECT t.TARGET_NAME,
               t.TARGET_TYPE,
               t.host_name,
               PROPERTY_NAME,
               PROPERTY_VALUE
          FROM SOL60237.TARGET_190305  t
               JOIN SOL60237.TARGET_PROPERTIES_190305 p
                   ON t.target_guid = p.target_guid
         WHERE     host_name = '&server'
               AND t.target_type = 'osm_cluster'
               AND property_name IN
                       ('ServiceName',
                        'ClusterName',
                        'orcl_gtp_lifecycle_status'))
       PIVOT (MAX (property_value)
             FOR property_name
             IN ('ServiceName' AS ServiceName,
                'ClusterName' AS ClusterName,
                'orcl_gtp_lifecycle_status' AS orcl_gtp_lifecycle_status))
"""

sqlAddListener="""
SELECT    'emcli add_target -name='
       || TARGET_NAME
       || ' -type=oracle_listener -host=&server -properties='
       || '"'
       || 'LsnrName:'
       || LsnrName
       || ';ListenerOraDir:'
       || ListenerOraDir
       || ';Port:'
       || Port
       || ';OracleHome:'
       || OracleHome
       || ';Machine:'
       || Machine
       || '"'
  FROM (SELECT TARGET_NAME, PROPERTY_NAME, PROPERTY_VALUE
          FROM SOL60237.TARGET_PROPERTIES_190305
         WHERE TARGET_NAME IN
                   (SELECT TARGET_NAME
                      FROM sol60237.target_190305
                     WHERE     host_name = '&server'
                           AND target_type = 'oracle_listener') --and property_name in ('LsnrName','ListenerOraDir','Port','OracleHome','Machine')
                                                               )
       PIVOT (MAX (property_VALUE)
             FOR property_name
             IN ('LsnrName' LsnrName,
                'ListenerOraDir' ListenerOraDir,
                'Port' Port,
                'OracleHome' OracleHome,
                'Machine' Machine)) add_target
"""

sqlAddOracleHome="""
SELECT    'emcli add_target -name='
       || TARGET_NAME
       || ' -type=oracle_home -host=&server -properties='
       || '"'
       || 'INSTALL_LOCATION:'
       || INSTALL_LOCATION
       || ';INVENTORY:'
       || INVENTORY
       || ';HOME_TYPE:'
       || HOME_TYPE
       || '"'
  FROM (SELECT TARGET_NAME, PROPERTY_NAME, PROPERTY_VALUE
          FROM SOL60237.TARGET_PROPERTIES_190305
         WHERE TARGET_NAME IN
                   (SELECT TARGET_NAME
                      FROM sol60237.target_190305
                     WHERE     host_name = '&server'
                           AND target_type = 'oracle_home')
                                                               )
       PIVOT (MAX (property_VALUE)
             FOR property_name
             IN ('INSTALL_LOCATION' INSTALL_LOCATION,'INVENTORY' INVENTORY,'HOME_TYPE' HOME_TYPE))
"""

strVypisAgenta="""
emcli get_targets -noheader -targets=&server%:oracle_emd -script
"""

strZastavAgenta="""
emcli stop_agent  -agent=&server:3872 -credential_name=ORACLE_SSH
"""

strOdeberAgenta="""
emcli delete_target -name=&server:3872 -type=oracle_emd -delete_monitored_targets -async
"""

strSmazOraagt="""
ssh &server 'rm -rf /oraagt/*'
"""

# oracle@oem:~$ emcli list_add_host_platforms

# AIX -platform=212
# Linux -platform=226
# HP-UX -platform=197
strInstallAgent="""
emcli submit_add_host -host_names=&server -platform=226 -installation_base_dir=/oraagt/agent13c -credential_name=ORACLE_SSH -credential_owner=POSTAK -wait_for_completion
"""

rootSh="""
ssh &server "echo  /oraagt/agent13c/agent_13.3.0.0.0/root.sh | sudo su - root"
"""

deployPluginOnAgent="""
emcli deploy_plugin_on_agent -agent_names=&server:3872 -plugin=oracle.sysman.db
"""

getPluginDeploymentStatus="""
emcli get_plugin_deployment_status -plugin=oracle.sysman.db
"""

listPlugins="""
emcli list_plugins_on_agent -agent_names=&server:3872 -include_discovery
"""

# functions
###########
def replaceEmcli(inputstr,toemFalseTrue):
    x=(base64.b64decode('UCNzdDZyZXFQI3N0NnJlcQ=='))
    x=str(x,'utf-8')
    inputstr = inputstr.replace("abcd1234", x)
    if toemFalseTrue:
        inputstr = inputstr.replace("emcli ", "/oraomst/Middleware_1303/bin/emcli ")
    else:
        inputstr = inputstr.replace("emcli ", "/oraoms/Middleware_1303/bin/emcli ")
    inputstr = inputstr.replace("&RacServerName1", RacServerName1)
    inputstr = inputstr.replace("&RacServerName2", RacServerName2)
    inputstr = inputstr.replace("&server", server)
    # inputstr = inputstr.replace("&sshToem", server)
    return inputstr

def connToOmsp():
    os.putenv("LD_LIBRARY_PATH", "/oracle/product/db/12.1.0.2/lib:/opt/rh/python33/root/usr/lib64")
    os.putenv("ORACLE_HOME","/oracle/product/db/12.1.0.2")
    os.putenv("ORACLE_SID", "OMSP")
    conn = cx_Oracle.connect("/", mode = cx_Oracle.SYSDBA)
    return conn

def runSql(sql):
    con=connToOmsp()
    cur = con.cursor()
    r = cur.execute(sql)
    rows = cur.fetchall()
    return rows

def runEmcli(sqlAddTarget,toemFalseTrue):
    sqlAddTarget=replaceEmcli(sqlAddTarget,toemFalseTrue)
    rows=runSql(sqlAddTarget)
    for row in rows:
        args=row[0]
        args="ssh toem '" + args + "'"
        print(args)
        subprocess.run([args],shell=True)

def runShell(args,sshToemFalseTrue):
    args=replaceEmcli(args,sshToemFalseTrue)
    if sshToemFalseTrue:
        args=sshToem(args)
    print(args)
    subprocess.run([args],shell=True)

def sshToem(strForSSH):
    strForSSH="ssh toem '" + strForSSH + "'"
    return strForSSH

def runRacOrSingle(cmdText,toemTrueFalse=False):
    global server
    if RAC:
        server=RacServerName1
        runShell(cmdText,toemTrueFalse)
        server=RacServerName2
        runShell(cmdText,toemTrueFalse)
    else:
        runShell(cmdText,toemTrueFalse)

tasks = []

def install_agent(pauseRun=False):
    # vypis agenta
    run_RacOrSingle(strVypisAgenta,False)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    ## na oem odeber agenta
    # zastav agenta
    runRacOrSingle(strZastavAgenta ,False)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # odeber agenta se vsemi targety (trva 30s)
    runRacOrSingle(strOdeberAgenta ,False)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # kontrola odebrani agenta
    runRacOrSingle(strVypisAgenta,False)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    ## na toem pridej host (trva 5 minut)
    # smaz /oraagt/* na cilovem serveru
    runRacOrSingle(strSmazOraagt ,False)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # nainstaluj agenta
    runRacOrSingle(strInstallAgent,True)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # root.sh
    runRacOrSingle(rootSh,False)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # deploy plugin
    runRacOrSingle(deployPluginOnAgent,True)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # getPluginDeploymentStatus
    runRacOrSingle(getPluginDeploymentStatus,True)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # list plugins
    runRacOrSingle(listPlugins,True)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")

def addTargetsRAC(pauseRun=False):
    global server
    # pridej cluster
    server=RacServerName1
    runEmcli(sqlAddCluster,True)
    server=RacServerName2
    runEmcli(sqlAddCluster,True)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # pridej instance
    server=RacServerName1
    runEmcli(sqlAddDatabaseInstance,True)
    server=RacServerName2
    runEmcli(sqlAddDatabaseInstance,True)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # pridej RAC
    server=RacServerName1
    runEmcli(sqlAddRacDatabase,True)
    server=RacServerName2
    runEmcli(sqlAddRacDatabase,True)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # pridej ASM instance
    server=RacServerName1
    runEmcli(addOsmInstances,True)
    server=RacServerName2
    runEmcli(addOsmInstances,True)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # pridej ASM cluster
    server=RacServerName1
    runEmcli(addOsmCluster,True)
    server=RacServerName2
    runEmcli(addOsmCluster,True)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # pridej listener
    server=RacServerName1
    runEmcli(sqlAddListener,True)
    server=RacServerName2
    runEmcli(sqlAddListener,True)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # pridej oracle home
    server=RacServerName1
    runEmcli(sqlAddOracleHome,True)
    server=RacServerName2
    runEmcli(sqlAddOracleHome,True)


def addTargetsSingleInstance(pauseRun=False):
    # pridej instance
    runEmcli(sqlAddDatabaseInstance,True)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # pridej ASM instance
    runEmcli(addOsmInstances,True)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # pridej listener
    runEmcli(sqlAddListener,True)
    if pauseRun: wait = input("PRESS ENTER TO CONTINUE.")
    # pridej oracle home
    runEmcli(sqlAddOracleHome,True)


# main
##########
## install agent
installAgent(False)


## pridej tgargety
# wait = input("PRESS ENTER TO CONTINUE install targets.")
# addTargetsRAC(False)
addTargetsSingleInstance(False)




