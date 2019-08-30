#!/usr/bin/env python3


""" Retry Redim ERROR tasks """

from __future__ import unicode_literals
from __future__ import print_function

__version__ = '2019-08-29'
__author__ = 'Jiri Srba'
__email__ = 'jsrba@csas.cz'
__status__ = 'Development'

import argparse
import logging
import os
import logging
import cx_Oracle
import numpy as np
import pandas as pd

# pripojeni k repository INFP pres DASHBOARD
DSN_INFP = 'dashboard/abcd1234@oem12.vs.csin.cz:1521/INFP'

ORACLE_ENV_VARIABLE = {
    'ORACLE_HOME': '/oracle/product/db/19',
    'LD_LIBRARY_PATH': '/oracle/product/db/19/lib',
    'TNS_ADMIN_DIR': '/etc/oracle/wallet/system',
    'NLS_LANG': 'AMERICAN_AMERICA.AL32UTF8'
}

# set logging
logging.basicConfig(level=logging.DEBUG)

def set_cx_oracle_env():
  """Set Oracle ENV"""

  # cx_Oracle variables
  for key, val in ORACLE_ENV_VARIABLE.items():
    if key.upper() not in os.environ:
      os.environ[key.upper()] = val


def get_redim_errors(db):

  sql = """select
        REDIM_PENDING_TASK.rowid,
        database,
        redim_username, redim_password, hostname, port,
        redim_package, task_name, task_params,
        sqlcode, status
      from REDIM_OWNER.REDIM_PENDING_TASK
            natural join REDIM_OWNER.redim_databases
      where status = 'ERROR'
        and database like :db
      order by sqlcode, database"""

  if db is None:
    db = '%'

  conn_infp = cx_Oracle.connect(DSN_INFP)
  cursor = conn_infp.cursor()

  cursor.execute(sql, { 'db': db })
  redim_error = cursor.fetchall()

  ## conn_infp.close()
  # cx_Oracle.DatabaseError: DPI-1054: connection cannot be closed when open statements or LOBs exist

  return redim_error


def parse_redim_parameter(param):
  """ Konverze Redim parametru do dict fomratu
      P_USER_NAME=cen85437,P_OPERATION_TYPE=0,P_DB_NAME=CRMDB"""

  param_dict = dict((x.strip(), y.strip()) for x, y in (element.split('=')
                                                  for element in param.split(',')))
  return param_dict


def call_pending_task(db_error):

  redim_method = db_error[7]

  dsn_tns = cx_Oracle.makedsn(
      db_error[4], db_error[5], db_error[1])

  logging.debug('dsn_tns: %s', dsn_tns)

  try:
    conn_db = cx_Oracle.connect(
        user=db_error[2], password=db_error[3], dsn=dsn_tns)

    cursor = conn_db.cursor()

    redim_package = '.'.join([db_error[6], db_error[7]])
    redim_call_params = parse_redim_parameter(db_error[8])
    logging.debug('redim_package: %s', redim_package)
    logging.debug('redim_call_params: %s', redim_call_params)

    if redim_method.upper() == 'GRANT_REVOKE_ROLE':
      # FIXME: nutno provolat pres vsechny odebirane role
      # vystup pridat do redim_call_params
      pass

    result = cursor.callproc(redim_package,
                             keywordParameters=redim_call_params)

    logging.debug('result: %s', result)

    conn_db.commit()
    cursor.close()
    conn_db.close()

  except cx_Oracle.DatabaseError as exc:
    # pouze vypis chybu a pokracuj
    logging.debug('ERROR db: %s', db_error[1])
    logging.debug('exception: %s', exc)

  """
  cursor = conn.cursor()
  cursor.callproc("so_test",
  """

def main():

  # parser na zjisteni hodnoty nazvu db, jinak vsechny db
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--db", help="database", action="store",
                dest="db")

  args = parser.parse_args()

  set_cx_oracle_env()

  # ziskam vsechny chyby pro danou db/vsechny db
  redim_errors = get_redim_errors(args.db)

  for db_error in redim_errors:
    call_pending_task(db_error)


if __name__ == "__main__":
  main()
