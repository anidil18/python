#!/usr/bin/env python3

"""
Jenkins build script to execute SQL file

USERNAME = / + connect string z REST API INFP

export TNS_ADMIN=/etc/oracle/wallet/sys
export SQLCL=sqlplus
"""

from __future__ import unicode_literals
from __future__ import print_function

import argparse
import logging
import os
import sys
from subprocess import Popen, PIPE
import yaml
import requests
from requests.auth import HTTPBasicAuth


__version__ = '1.2'
__author__ = 'Jiri Srba'
__status__ = 'Development'

logging.basicConfig(level=logging.DEBUG)

RESTRICTED_SQL = [
    'GRANT',
    'REVOKE',
    'CREATE USER',
    'DROP USER',
    'DBA',
    'SYSDBA',
    'ALTER SYSTEM',
    'CREATE DATABASE',
    'DROP DATABASE']

# INFP Rest API
INFP_REST_OPTIONS = {
    'url': 'https://oem12-m.vs.csin.cz:1528/ords/api/v1/db',
    'user': 'dashboard',
    'pass': 'abcd1234'}

# JIRA
JIRA_REST_OPTIONS = {
    'server': 'https://jira.atlassian.com'}

# Oracle wallet for SYS
TNS_ADMIN_DIR = '/etc/oracle/wallet'
SQLCL = 'sqlplus'


def get_jira_issue(jira_rest_options, jira_issue):
  """Get info from JIRA ticket"""
  pass


def read_yaml_config(config_file):
  """Read YAML config file"""
  try:
    with open(config_file, 'r') as stream:
      return yaml.load(stream)
  except (IOError, yaml.YAMLError) as exc:
    logging.error(exc)
    raise


def get_db_info(infp_rest_options, dbname):
  """Rest API call to INFP"""

  # CA cert file
  if sys.platform.startswith('linux'):
    verify = '/etc/ssl/certs/ca-bundle.crt'
  else:
    verify = False

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


def check_for_app(db_app, app):
  """Kontrola, zda je databaze registrovana v OLI pro danou APP"""
  if app.upper() not in db_app:
    raise ValueError(
        'Databaze neni registrovana pro aplikaci {app}.'
        .format(app=app))


def check_for_env_status(env_status):
  """Kontrola, zda neni database registrovana jako produkcni"""
  if 'Production' in env_status:
    raise AssertionError(
        'Databaze je registrovana jako produkcni, exitting')


def check_for_restricted_sql(script):
  """Kontrola na restricted SQL"""
  with open(script, 'r') as f:
    for line in f:
      for sql in RESTRICTED_SQL:
        if sql.upper() in line.rstrip().upper():
          raise AssertionError('Restricted SQL: {} in line {}'.format(sql, line))


def execute_sql_script(sqlcl_connect_string, sql_file):
  """Run SQL script with connect description"""

  sqlplus = Popen(sqlcl_connect_string.split(), stdin=PIPE,
                  stdout=PIPE, stderr=PIPE, universal_newlines=True)
  sqlplus.stdin.write('@' + sql_file)
  return sqlplus.communicate()


def execute_db(dbname, cfg):
  """Execute SQL againt dbname"""

  logging.debug('execute dbname %s with cfg: %s', dbname, cfg)

  dbinfo = get_db_info(INFP_REST_OPTIONS, dbname)
  logging.info('dbinfo: %s', dbinfo)

  # check for production env
  check_for_env_status(dbinfo['env_status'])

  # assert app
  if cfg['variables']['app']:
    check_for_app(dbinfo['app_name'], cfg['variables']['app'])

  # create sqlplus command with connect string
  sqlcl_connect_string = cfg['sqlcl'] + ' /@//' + dbinfo['connect_descriptor']
  if 'SYS' in cfg['variables']['username'].upper():
    sqlcl_connect_string += ' AS SYSDBA'

  for script in cfg['script']:
    result = execute_sql_script(sqlcl_connect_string, script)
    if result:
      # print sqlplus result with newlines
      for line in result:
        print(line, end='')


def main(args):
  """ Main function """

  cfg = {
      'variables': {
          'dbname': None,
          'app': None,
          'username': 'SYS'},
      'stage': ['deploy'],
      'script': [],
      'jira': None}

  logging.debug('args: %s', args)
  logging.debug('cfg: %s', cfg)

  # override cfg with config file
  if args.config_file:
    cfg.update(read_yaml_config(args.config_file))
    logging.debug('cfg update with yaml: %s', cfg)

  # override dbname
  if args.dbname:
    cfg['variables']['dbname'] = args.dbname
  logging.debug('dbname: %s', cfg['variables']['dbname'])

  # assert na specifikovany dbname
  if not cfg['variables']['dbname']:
    raise AssertionError("Value dbname not specified")

  # override with JIRA ticket
  if args.jira_issue:
    cfg['jira'] = args.jira_issue
    logging.debug('jira: %s', cfg['jira'])

  # override sql filename z argv
  if args.script:
    cfg['script'] = [' '.join(args.script)]
  logging.info('sql script file: %s', cfg['script'])

  # assert na specifikovany dbname
  if not cfg['script']:
    raise AssertionError("SQL script filename not specified")

  # check for restricted SQL operation
  for script in cfg['script']:
    check_for_restricted_sql(script)

  # override and check for APP if defined
  if args.app_name:
    cfg['variables']['app'] = args.app_name

  # override username
  if args.username:
    cfg['variables']['username'] = args.username.upper()

  # read ENV config variables
  if 'SQLCL' in os.environ:
    cfg['sqlcl'] = os.environ['SQLCL']
  else:
    cfg['sqlcl'] = SQLCL

  if 'TNS_ADMIN' not in os.environ:
    os.environ['TNS_ADMIN'] = os.path.join(
        TNS_ADMIN_DIR, cfg['variables']['username'].lower())

  # iterate over databases
  for dbname in list(filter(str.isalnum, cfg['variables']['dbname'])):
    execute_db(dbname, cfg)


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description="Jenkins build script to execute SQL file")
  parser.add_argument('-d', '--db', action="store", dest="dbname",
                      help="dbname")
  parser.add_argument('-u', '--user', action="store", dest="username",
                      help="Username to connect")
  parser.add_argument('--app', action="store", dest="app_name",
                      help="SAS App name")
  parser.add_argument('--config', action="store", dest="config_file",
                      help="CI configuration YAML file")
  parser.add_argument('--jira', action="store", dest="jira_issue",
                      help="jira ticket issue")
  parser.add_argument('script', metavar='sql filename', type=str, nargs='*',
                      default=None, help='SQL script to execute')
  arguments = parser.parse_args()
  main(arguments)
