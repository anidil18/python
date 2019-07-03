#!/usr/bin/env python3

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

    ## 2019-06-18
    - Change Oracle version to 19.3

    ## 2019-05-10
    - Add run EP-xxx.sql, pokud se podari ziskat ho z description
    - Add run SQL attachment, pokud se ho podari stahnout

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
import multiprocessing
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

# spust SQL skript
DEBUG = False

logging.basicConfig(level=logging.WARNING)
##logging.basicConfig(level=logging.DEBUG)

# Oracle ENV, $ORACLE_HOME, TNS admin pro SYS wallet
DEFAULT_ENV_VARIABLE = {
    'ORACLE_HOME': '/oracle/product/db/19',
    'SQLCL': '/oracle/product/db/19/bin/sqlplus -L',
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
# SQLcl: nutno doplnit o prefix Error Message =
ORACLE_EXCEPTIONS = (
    'ORA-01017: invalid username/password',
    'ORA-01804: failure to initialize timezone information',
    'ORA-12514: TNS:listener does not currently know of service requested in connect descriptor'
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

# global timestamp pro vsechny generovane logy
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

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


def get_logfile(sql_script, dbname):
  """Get name of logfile"""

  log_file = '.'.join([os.path.basename(sql_script), dbname, TIMESTAMP, 'log'])
  logging.debug('log_file: %s', log_file)
  return log_file


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


def get_jira_file(jira_issue):
  """return JIRA sql file from JIRA ticket"""
  return '.'.join([jira_issue, 'sql'])


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
    jira_file = get_jira_file(jira_issue)
    with open(jira_file, 'w') as fd:
      logging.info('creating file: %s', jira_file)
      fd.writelines(text)

  # return code dle toho, zda se ne-podarilo
  return bool(text)


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

  # fix pro vyprsely oem12 certifikat
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
        'Databaze {} neni registrovana v OLI nebo neni nastaven lifecycle v OEM'
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


def check_for_error(log_file):
  """ Kontrola na ORA- a SP- chyby"""

  ora_errors = []

  with open(log_file, 'r') as fd:
    for line in fd:
      # parse ORA- errors
      if line.rstrip().startswith(ORACLE_ERRORS):
        ora_errors.append(line)

  return ora_errors

def execute_sql_script(dbname, connect_string, sql_script, log_file):
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
        '|'||
        to_char(sysdate, 'YYYY-MM-DD"T"HH24:MI:SS') ||'|'
        || name || '|'
          as "|timestamp|database_name|"
      from v$database;
    ''' + os.linesep)
  session.stdin.write('@"{}"'.format(sql_script))

  (stdout, stderr) = session.communicate()

  # FIXME: sqlplus pri chybe nevraci STDERR !
  if stderr:
    raise ValueError('SQL script {} failed with error: {}'.format(
        sql_script, stderr))

  if stdout:
    with open(log_file, 'w') as fd:
      # print and save sqlplus results with newlines
      for line in stdout.splitlines():

        # print to screen in DEBUG mode
        if DEBUG:
          print(line)

        # and save to file
        print(line + '\n', file=fd)
        # fd.write(line + '\n')

        # SQLcl: nutno provest strip na Error Message =
        if line.strip().strip('Error Message = ').startswith(ORACLE_EXCEPTIONS):
          raise OracleCIError('connection failed to {}'.format(dbname))

        # parse ORA- errors
        if line.rstrip().startswith(ORACLE_ERRORS):
          ora_errors.append(line)

  # logging.debug('ora_errors: {}'.format(ora_errors))
  return ora_errors


def run_db(dbname, cfg, check_prod):
  """Execute SQL againt dbname"""

  ora_errors = []

  logging.info('dbname: %s', dbname)

  dbinfo = get_db_info(INFP_REST_OPTIONS, dbname)
  logging.debug('dbinfo: %s', dbinfo)

  # check for production env
  if check_prod:
    check_for_env_status(dbinfo['env_status'])

  # assert app
  if cfg['variables']['app']:
    check_for_app(dbinfo['app_name'], cfg['variables']['app'])

  # JIRA ticket
  if cfg['jira']:
    jira_issue = cfg['jira']
  else:
    jira_issue = None

  # generate sqlplus command with connect string
  connect_string = cfg['sqlcl'] + ' /@//' + dbinfo['connect_descriptor']
  if 'SYS' in cfg['variables']['user'].upper():
    connect_string += ' AS SYSDBA'

  for sql_script in convert_to_dict(cfg['script']):
    # generate log_file pro zapis
    log_file = get_logfile(sql_script, dbname)

    ora_error = execute_sql_script(dbname, connect_string, sql_script,
                                    log_file)

    # upload attachment to JIRA ticket
    if cfg['jira']:
      jira_upload_attachment(cfg['jira'], log_file)

    # extend ora_error
    if ora_error:
      ora_errors.extend(ora_error)

  return ora_errors


def main(args):
  """ Main function """

  # default konfigurace, pokud neni specifikovano jinak
  # <cfg.db> vs <cfg.database>, podporuje obe specifikace
  # <cfg.database> má vetši prioritu, prepise nastaveni <cfg.db>
  cfg = {
      'variables': {
          'database': None,
          'db': None,
          'app': None,
          'user': 'SYS'},
      'stage': ['deploy'],
      'script': [],
      'jira': None}

  logging.debug('args: %s', args)

  # zachyceni ORA- a SP- errors
  ora_errors = []

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
    if jira_description(args.jira, str(jira_ticket['fields']['description'])):
      # override SQL filename na <JIRA>.sql
      jira_file = get_jira_file(args.jira)
      cfg['script'] = [jira_file]

    for attachment in jira_ticket['fields']['attachment']:
      logging.info('jira file attachment: %s', attachment['filename'])

      # FIXME: check for space in attachment['filename']

      jira_dowload_attachment(attachment)
      # add SQL script to list cfg['script']
      if attachment['filename'].lower().endswith('.sql'):
        cfg['script'].append(attachment['filename'])

  # override cfg.db na cfg.database
  if cfg['variables']['database'] is None:
    cfg['variables']['database'] = cfg['variables']['db']

  # override variables from args
  for var in cfg['variables']:
    # db var neprepisuju
    if 'db' not in var:
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

  # print SQL skript to run
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

  logging.debug('cfg: %s', cfg)

  # iterate over ALL databases
  databases = convert_to_dict(cfg['variables']['database'])

  if args.parallel:
    # parallel multiprocessing
    all_processes = [multiprocessing.Process(target=run_db, args=(
        dbname, cfg, args.check_prod, )) for dbname in databases]

    # start
    for p in all_processes:
      p.start()

    # wait for finish
    for p in all_processes:
      p.join()

  else:
    # SERIAL
    for dbname in databases:
      run_db(dbname, cfg, args.check_prod)

  # check for errors from log_file seriově, aby fungoval parallel
  for dbname in databases:
    for sql_script in convert_to_dict(cfg['script']):
      # generate log_file pro zapis
      log_file = get_logfile(sql_script, dbname)

      # kontrola a zapis do ora_errors[]
      ora_error = check_for_error(log_file)
      if ora_error:
        ora_errors.extend(ora_error)

  # vypis ORA- / SP- errors
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
  parser.add_argument('-p', '--parallel', action="store_true", dest="parallel",
                      help="parallel processing")
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
