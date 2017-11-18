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
import os
import sys
import yaml
import logging
from subprocess import Popen, PIPE
import requests
from requests.auth import HTTPBasicAuth


__version__ = '1.2'
__author__ = 'Jiri Srba'
__status__ = 'Development'


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# INFP Rest API
INFP_REST_OPTIONS = {
    'url': 'https://oem12.vs.csin.cz:1528/ords/api/v1/db',
    'user': 'dashboard',
    'pass': 'abcd1234'}

# JIRA
JIRA_REST_OPTIONS = {
    'server': 'https://jira.atlassian.com'}

# Oracle wallet for SYS
TNS_ADMIN_DIR = '/etc/oracle/wallet'

def get_jira_issue(jira_rest_options, jira_issue):
  """Get info from JIRA ticket"""
  pass


def read_ci_config(cfg, config_file):
  """Read YAML config file and update cfg dict"""

  with open(config_file) as stream:
    try:
      cfg.update(yaml.load(stream))
    except yaml.YAMLError as exc:
      logger.error(exc)
      raise
  return cfg

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


def check_for_app(dbinfo, app):
  """Kontrola, zda je databaze registrovana v OLI pro danou APP"""
  if app.upper() not in dbinfo['app_name']:
    raise ValueError(
        'Databaze {db} neni registrovana pro aplikaci {app}.'
        .format(db=dbinfo['dbname'], app=app))


def check_for_env_status(dbinfo):
  """Kontrola, zda neni database registrovana jako produkcni"""
  if 'Production' in dbinfo['env_status']:
    raise AssertionError(
        'Databaze {} je registrovana jako produkcni.'.format(dbinfo))


def run_sql_script(sqlcl_string, sql_file):
  """Run SQL script with connect description"""

  sqlplus = Popen(sqcl_string.split(), stdin=PIPE,
                  stdout=PIPE, stderr=PIPE, universal_newlines=True)
  sqlplus.stdin.write('@' + sql_file)
  return sqlplus.communicate()


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

  logger.debug('args: %s', args)
  logger.debug('cfg: %s', cfg)

  # parse config file
  if args.config_file:
    cfg = read_ci_config(cfg, args.config_file)
    logging.debug('cfg: %s', cfg)

  # overwrite dbname
  if args.dbname:
    cfg['variables']['dbname'] = args.dbname

  # nacteni sql filename z argv[1]
  if args.script:
    cfg['script'] = [' '.join(args.script)]

  if args.jira_issue:
    cfg['jira'] = args.jira_issue
    logging.debug('jira: %s', cfg['jira'])

  if not cfg['variables']['dbname']:
    raise AssertionError("Value dbname not specified")

  dbinfo = get_db_info(INFP_REST_OPTIONS, cfg['variables']['dbname'])
  logging.info('dbinfo: %s', dbinfo)
  logging.info('sql script file: %s', cfg['script'])

  # check for production env
  check_for_env_status(dbinfo)

  # check for APP if defined
  if args.app_name:
    cfg['variables']['app'] = args.app_name

  if cfg['variables']['app']:
    check_for_app(dbinfo, args.app_name)

  if args.username:
    cfg['variables']['username'] = args.username.upper()

  # read ENV config variables
  if 'SQLCL' in os.environ:
    sqlcl_string = os.environ['SQLCL']
  else:
    sqlcl_string = 'sqlplus'

  if 'TNS_ADMIN' not in os.environ:
    os.environ['TNS_ADMIN'] = os.path.join(
        TNS_ADMIN_DIR, cfg['variables']['username'].lower())

  # create sqlplus command with connect string
  sqlcl_string += '/@//' + dbinfo['connect_descriptor']
  if 'SYS' in cfg['variables']['username']:
    sqlcl_string += ' AS SYSDBA'

  for f in cfg['script']:
    result = run_sql_script(sqlcl_string, f)
    if result:
      # print sqlplus result with newlines
      for line in result:
        print(line, end='')


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
  parser.add_argument('script', metavar='script', type=str, nargs='+',
                      default=None, help='SQL script to execute')
  args = parser.parse_args()
  main(args)
