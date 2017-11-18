#!/usr/bin/env python3

"""
Jenkins build script to execute SQL file

USERNAME = / + connect string z REST API INFP

export TNS_ADMIN=/etc/oracle/wallet/sys

export APP=ORDBF
export SQLCL=sqlplus
"""

from __future__ import unicode_literals
from __future__ import print_function

import argparse
import os
from subprocess import Popen, PIPE
import requests
from requests.auth import HTTPBasicAuth


__version__ = '1.2'
__author__ = 'Jiri Srba'
__status__ = 'Development'

# INFP Rest API
INFP_REST_OPTIONS = {
    'url': 'https://oem12.vs.csin.cz:1528/ords/api/v1/db',
    'user': 'dashboard',
    'pass': 'abcd1234'}

# JIRA
JIRA_REST_OPTIONS = {
    'server': 'https://jira.atlassian.com'}

# Oracle wallet for SYS
TNS_ADMIN_SYS = '/etc/oracle/wallet/sys'

def get_jira_issue(jira_rest_options, jira_issue):
  """Get info from JIRA ticket"""
  pass


def get_db_info(infp_rest_options, dbname):
  """Rest API call to INFP"""

  # CA cert file
  verify = '/etc/ssl/certs/ca-bundle.crt'

  r = requests.get(
      '/'.join([infp_rest_options['url'], dbname]),
      auth=HTTPBasicAuth(
          infp_rest_options['user'], infp_rest_options['pass']),
      verify=verify
  )
  try:
    return r.json()
  except ValueError:  # includes simplejson.decoder.JSONDecodeError
    raise ValueError(
        'Databaze {} neni registrovana v OLI nebo ma nastaven spatny GUID'
        .format(dbname))


def check_for_app(dbinfo, app):
  """Kontrola, zda je databaze registrovana v OLI pro danou APP"""
  if app not in dbinfo['app_name']:
    raise ValueError(
        'Databaze {db} neni registrovana pro aplikaci {app}.'
        .format(db=dbinfo['dbname'], app=app))


def check_for_env_status(dbinfo):
  """Kontrola, zda neni database registrovana jako produkcni"""
  if 'Production' in dbinfo['env_status']:
    raise ValueError(
        'Databaze {} je registrovana jako produkcni.'.format(dbinfo))


def run_sql_script(sqlcl, connect_string, sql_filename):
  """Run SQL script with connect description"""

  sqlplus = Popen([sqlcl, connect_string], stdin=PIPE,
                  stdout=PIPE, stderr=PIPE, universal_newlines=True)
  sqlplus.stdin.write('@' + sql_filename)
  return sqlplus.communicate()


def main(dbname, sql_script, jira_issue):
  """ Main function """

  # nacteni sql filename z argv[1]
  sql_filename = ' '.join(sql_script)

  dbinfo = get_db_info(INFP_REST_OPTIONS, dbname)
  print('dbinfo: {}'.format(dbinfo))
  print('sql script file: {}'.format(sql_filename))

  # check for production env
  check_for_env_status(dbinfo)

  # check for APP if defined
  if 'APP' in os.environ:
    app = os.environ['APP']
    check_for_app(dbinfo, app)

  if 'SQLCL' in os.environ:
    sqlcl = os.environ['SQLCL']
  else:
    sqlcl = 'sqlplus'

  if 'TNS_ADMIN' not in os.environ:
    os.environ['TNS_ADMIN'] = TNS_ADMIN_SYS

  connect_string = '/@//' + dbinfo['connect_descriptor'] + ' AS SYSDBA'

  sql_result = run_sql_script(sqlcl, connect_string, sql_filename)
  if sql_result:
    # print sqlplus result with newlines
    for line in sql_result:
      print(line, end='')


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description="Jenkins build script to execute SQL file")
  parser.add_argument('sql_script', metavar='sql_script', type=str, nargs='+',
                      help='SQL script to execute')
  parser.add_argument('-d', '--db', action="store", dest="dbname",
                      required=True, help="dbname")
  parser.add_argument('-j', '--jira', action="store", dest="jira_issue",
                      help="jira ticket issue")
  args_result = parser.parse_args()
  main(args_result.dbname, args_result.sql_script, args_result.jira_issue)
