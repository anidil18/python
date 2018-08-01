#!/usr/bin/env python3
# coding: utf-8

"""
    Jenkins build script to execute SQL file

    # requirements.txt
    pip install pyyaml

    # Usage:
    USERNAME = / + connect string z REST API INFP
    export SQLCL=sqlplus

    # Changelog:
    ## Not implemented
    - Add podpora pro dalsi username mimo SYS, napr. SYSTEM, DBSNMP

    ## 2018-07-31
    - Change rozdeleni switch --check-prod a --no-check pro SQL a env PROD

    ## 2018-04-21
    - Add upload log to JIRA attachments

    ## 2018-02-17
    - Rename file to oracle-ci-deploy.py
    - Add parse sql output, detekce na ORA- a SP2-
    - Change env prostředí JDK, TNS a sqlcl jako dict proměnnou
"""

from __future__ import unicode_literals
from __future__ import print_function

import argparse
import logging
import os
import sys
import re
from subprocess import Popen, PIPE
from collections import Counter
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
import yaml


__version__ = '1.5'
__author__ = 'Jiri Srba'
__email__ = 'jsrba@csas.cz'
__status__ = 'Development'

logging.basicConfig(level=logging.DEBUG)

# Oracle ENV, $ORACLE_HOME, TNS admin pro SYS wallet
DEFAULT_ENV_VARIABLE = {
    'ORACLE_HOME': '/oracle/product/db/12.1.0.2',
    'SQLCL': '/oracle/product/db/12.1.0.2/bin/sqlplus -L',
    'TNS_ADMIN_DIR': '/etc/oracle/wallet'
}

RESTRICTED_SQL = (
    'PROFILE DEFAULT',
    'GRANT DBA',
    'SYSDBA',
    'ALTER SYSTEM SET',
    'NOAUDIT',
    'SHUTDOWN'
)

# ORACLE errors to raise exception
ORACLE_EXCEPTIONS = (
    'ORA-01017: invalid username/password',
    'ORA-01804: failure to initialize timezone information'
)

# Check for sqlplus errors
ORACLE_ERRORS = (
    'ORA-',
    'SP2-'
)

# INFP Rest API
INFP_REST_OPTIONS = {
    'url': 'https://oem12.vs.csin.cz:1528/ords/api/v1/db',
    'user': 'dashboard',
    'pass': 'abcd1234'
    }

# JIRA Rest API
JIRA_REST_OPTIONS = {
    'base_url': 'https://jiraprod.csin.cz/rest/api/2',
    'project': 'EP',
    'user': 'admin_ep',
    'pass': 'e4130J17P'
    }


class OracleCIError(Exception):
  """Jenkins Oracle CI Exception"""
  pass


def counter(a):
  """Sort and Count list ORA errors"""
  return Counter(a)


def convert_to_dict(value):
  """Convert str to dict"""
  if not isinstance(value, list):
    # convert separator to space and split into dict()
    value = ''.join(c if str.isalnum(c) else ' ' for c in value).split()
  return value


def get_jira_issue(jira_issue):
  """Get info from JIRA ticket"""

  auth = HTTPBasicAuth(JIRA_REST_OPTIONS['user'], JIRA_REST_OPTIONS['pass'])
  headers = {'Accept': 'application/json'}

  url = '/'.join([JIRA_REST_OPTIONS['base_url'], 'issue', jira_issue])

  resp = requests.get(url, headers=headers, auth=auth, verify=False)
  # logging.debug('resp: %s', resp.json())
  return resp.json()

def jira_dowload_attachment(attachment):
  """Download JIRA attachment"""

  url = attachment['content']
  auth = HTTPBasicAuth(JIRA_REST_OPTIONS['user'], JIRA_REST_OPTIONS['pass'])
  resp = requests.get(url, auth=auth, stream=True, verify=False)

  if resp.status_code == 200:
    with open(attachment['filename'], 'wb') as fd:
      for chunk in resp.iter_content(chunk_size=128):
        fd.write(chunk)


def jira_upload_attachment(jira_issue, jira_attachment):
  """Upload JIRA attachment"""

  url = '/'.join([JIRA_REST_OPTIONS['base_url'], 'issue', jira_issue,
                  'attachments'])

  auth = HTTPBasicAuth(JIRA_REST_OPTIONS['user'], JIRA_REST_OPTIONS['pass'])
  headers = {'X-Atlassian-Token': 'nocheck'}
  files = {'file': open(jira_attachment, 'rb')}
  logging.debug('file: %s', jira_attachment)

  resp = requests.post(url, auth=auth, files=files,
                       headers=headers, verify=False)

  logging.debug('resp.status_code: %s', resp.status_code)

  if resp.status_code == 200:
    # logging.debug('resp.text: %s', resp.text)
    logging.info('log file %s uploaded to JIRA %s', jira_attachment, jira_issue)


def jira_description(jira_issue, jira_desc):
  """Get SQL text from JIRA description and write to file"""

  # match {code} a {noformat}
  # regex = re.compile(
  #    r'\{(?:code|noformat)\}\r\n(.*)\r\n\{(?:code|noformat)\}', re.MULTILINE)

  regex = re.compile(r'\{(code.*|noformat)\}')

  text = ''
  grab_line = False

  for line in jira_desc.splitlines():
    # logging.debug('line %s', line)
    # logging.debug('grab_line %s', grab_line)

    if re.match(regex, line):
      # toogle grab line
      grab_line = True if grab_line is False else False

    if grab_line and not re.match(regex, line):
      text += line + '\n'

  # text = regex.findall(jira_desc)
  logging.debug('jira sql description: %s', text)

  if text:
    jira_file = '.'.join([jira_issue, 'sql'])
    with open(jira_file, 'w') as fd:
      logging.info('creating file: %s', jira_file)
      fd.writelines(text)


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

  resp = requests.get(
      '/'.join([infp_rest_options['url'], dbname]),
      auth=HTTPBasicAuth(
          infp_rest_options['user'], infp_rest_options['pass']),
      verify=verify
  )
  try:
    return resp.json()
  except ValueError:      # includes simplejson.decoder.JSONDecodeError
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
  with open(script, 'r', encoding="ascii", errors="surrogateescape") as fd:
    for line in fd:
      for sql in RESTRICTED_SQL:
        if sql.upper() in line.rstrip().upper():
          raise AssertionError(
              'Restricted SQL: {} found on line {}'.format(sql, line))


def execute_sql_script(dbname, connect_string, sql_script, jira_issue):
  """
  Run SQL script with connect description

  :return: ora_errors
  """

  # parse ORA errors
  ora_errors = []

  session = Popen(connect_string.split(),
                  stdin=PIPE,
                  stdout=PIPE,
                  stderr=PIPE,
                  universal_newlines=True)
  session.stdin.write('''
    select
      to_char(sysdate, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') ||':'|| name
        as DBINFO
      from v$database;
    ''' + os.linesep)
  session.stdin.write('@' + sql_script)

  (stdout, stderr) = session.communicate()

  # FIXME: sqlplus pri chybe nevraci STDERR !
  if stderr:
    raise ValueError('SQL script {} failed with error: {}'.format(
        sql_script, stderr))

  if stdout:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = '.'.join([sql_script, dbname, timestamp, 'log'])
    with open(log_file, 'w') as fd:
      # print and save sqlplus results with newlines
      for line in stdout.splitlines():

        # print to screen and save to file
        print(line)
        print(line + '\n', file=fd)
        # fd.write(line + '\n')

        if line.strip().startswith(ORACLE_EXCEPTIONS):
          raise OracleCIError('connection failed to {}'.format(dbname))

        # parse ORA- errors
        if line.rstrip().startswith(ORACLE_ERRORS):
          ora_errors.append(line)

    if jira_issue:
      jira_upload_attachment(jira_issue, log_file)

  # logging.debug('ora_errors: {}'.format(ora_errors))
  return ora_errors


def run_db(dbname, sql_script, cfg, check_prod, jira_issue=None):
  """Execute SQL againt dbname"""

  logging.info('dbname: %s', dbname)

  dbinfo = get_db_info(INFP_REST_OPTIONS, dbname)
  logging.debug('dbinfo: %s', dbinfo)

  # check for production env
  if check_prod:
    check_for_env_status(dbinfo['env_status'])

  # assert app
  if cfg['variables']['app']:
    check_for_app(dbinfo['app_name'], cfg['variables']['app'])

  # generate sqlplus command with connect string
  connect_string = cfg['sqlcl'] + ' /@//' + dbinfo['connect_descriptor']
  if 'SYS' in cfg['variables']['user'].upper():
    connect_string += ' AS SYSDBA'

  ora_errors = execute_sql_script(dbname, connect_string, sql_script,
                                  jira_issue)
  return ora_errors


def main(args):
  """ Main function """

  # default konfigurace, pokud neni specifikovano jinak
  cfg = {
      'variables': {
          'database': None,
          'app': None,
          'user': 'SYS'},
      'stage': ['deploy'],
      'script': [],
      'jira': None}

  logging.debug('args: %s', args)
  logging.debug('cfg: %s', cfg)

  # override cfg with config file
  if args.config_file:
    cfg.update(read_yaml_config(args.config_file))
    logging.debug('cfg update with yaml: %s', cfg)

  # override with JIRA ticket
  if args.jira:
    jira_ticket = get_jira_issue(args.jira)
    logging.info('jira database: %s', jira_ticket['fields']['customfield_18907'])
    cfg['variables']['database'] = jira_ticket['fields']['customfield_18907']

    # get SQL text from description
    jira_description(args.jira, str(jira_ticket['fields']['description']))

    for attachment in jira_ticket['fields']['attachment']:
      logging.info('jira file attachment: %s', attachment['filename'])
      jira_dowload_attachment(attachment)

  # override variables
  for var in cfg['variables']:
    if getattr(args, var):
      cfg['variables'][var] = getattr(args, var)
    logging.debug('var %s: %s', var, cfg['variables'][var])


  # assert na specifikovany dbname
  if not cfg['variables']['database']:
    raise AssertionError("Database name not specified")

  # override with JIRA ticket
  if args.jira:
    cfg['jira'] = args.jira
    logging.debug('jira: %s', cfg['jira'])

  # override sql filename z argv
  if args.script:
    cfg['script'] = [' '.join(args.script)]
  logging.info('sql script file: %s', cfg['script'])

  # assert na specifikovany dbname
  if not cfg['script']:
    raise AssertionError("SQL script filename not specified")

  # check for restricted SQL operation
  if args.check_sql:
    for script in convert_to_dict(cfg['script']):
      check_for_restricted_sql(script)

  # read ENV config variables
  for key, val in DEFAULT_ENV_VARIABLE.items():
    if key.upper() in os.environ:
      cfg[key.lower()] = os.environ[key]
    else:
      cfg[key.lower()] = val
      os.environ[key.upper()] = val

  # set TNS_ADMIN dle cfg user
  if 'TNS_ADMIN' not in os.environ:
    os.environ['TNS_ADMIN'] = os.path.join(
        DEFAULT_ENV_VARIABLE['TNS_ADMIN_DIR'], cfg['variables']['user'].lower())

  # iterate over ALL databases
  ora_errors = []
  for dbname in convert_to_dict(cfg['variables']['database']):
    for sql_script in convert_to_dict(cfg['script']):
      ora_error = run_db(dbname, sql_script, cfg, args.check_prod, cfg['jira'])
      if ora_error:
        ora_errors.extend(ora_error)

  # check for ORA- errors
  if ora_errors:
    logging.warning('ORA- errors found')
    # logging.debug('ora_errors: %s', ora_errors)
    for key, value in counter(ora_errors).most_common():
      print("{}x : {} ".format(value, key))


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description="Jenkins build script to execute SQL file")
  parser.add_argument('-d', '--db', action="store", dest="database",
                      help="dbname")
  parser.add_argument('-u', '--user', action="store", dest="user",
                      help="Username to connect")
  parser.add_argument('--app', action="store", dest="app",
                      help="SAS App name")
  parser.add_argument('--config', action="store", dest="config_file",
                      help="CI configuration YAML file")
  parser.add_argument('--jira', action="store", dest="jira",
                      help="jira ticket issue")
  parser.add_argument('--no-check', action="store_false", dest="check_sql",
                      help="do not check for restricted SQL")
  parser.add_argument('--check-prod', action="store_true", dest="check_prod",
                      help="check for prod env")
  parser.add_argument('script', metavar='sql filename', type=str, nargs='*',
                      default=None, help='SQL script to execute')
  arguments = parser.parse_args()
  main(arguments)
