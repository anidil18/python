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

# class pro pĹ™ipojenĂ­ k Oracle databĂˇzi


class Oracle(object):
  """public class for Connect to Oracle"""

  def __init__(self):
    # cx_Oracle variables
    for key, val in ORACLE_ENV_VARIABLE.items():
      if key.upper() not in os.environ:
        os.environ[key.upper()] = val

    self.db = cx_Oracle.connect(DSN_INFP)
    self.cursor = self.db.cursor()

  def __exit__(self, type, value, traceback):
    """close cursor and connections"""
    self.cursor.close()
    self.db.close()

  def conn(self):
    """return cx connection"""
    return self.db

  def callproc(self, name, parameters=None, commit=True):
    """Call PL/SQL procedure"""

    self.cursor.callproc(name, parameters)

    # Only commit if it-s necessary.
    if commit:
      self.db.commit()

  def execute(self, sql, bindvars=None, commit=False):
    """
    Execute whatever SQL statements are passed to the method;
    commit if specified. Do not specify fetchall() in here as
    the SQL statement may not be a select.
    bindvars is a dictionary of variables you pass to execute.
    """

    result = self.cursor.execute(sql, bindvars)

    # commit if it-s necessary.
    if commit:
      self.db.commit()

    return result

  def get_redim_errors(self, db):

    sql = """select
          request_id,
          database,
          redim_username, redim_password, hostname, port,
          redim_package, task_name, task_params,
          sqlcode, status
        from REDIM_OWNER.REDIM_PENDING_TASK
              natural join REDIM_OWNER.redim_databases
        where status = 'ERROR'
        --  and task_name = 'GRANT_REVOKE_ROLE'
          and database like :db
        order by sqlcode, database"""

    if db is None:
      db = '%'

    self.cursor.execute(sql, {'db': db})
    redim_error = self.cursor.fetchall()

    return redim_error

  def update_pending_task(self, request_id):
    """set pending task to status OK"""

    sql = """update REDIM_OWNER.REDIM_PENDING_TASK
            set status = 'OK',
            last_update_date = sysdate
            where request_id = :id"""
    self.cursor.execute(sql, { 'id': request_id})
    self.db.commit()


def parse_redim_parameter(param):
  """ Konverze Redim parametru do dict fomratu
      P_USER_NAME=cen85437,P_OPERATION_TYPE=0,P_DB_NAME=CRMDB"""

  param_dict = dict((x.strip(), y.strip()) for x, y in (element.split('=')
                                                  for element in param.split(',')))
  return param_dict


def call_pending_task(db_error):

  redim_db = db_error[1]
  redim_method = db_error[7]

  dsn_tns = cx_Oracle.makedsn(
      db_error[4], db_error[5], redim_db)

  logging.debug('dsn_tns: %s', dsn_tns)

  try:
    conn_db = cx_Oracle.connect(
        user=db_error[2], password=db_error[3], dsn=dsn_tns)

    cursor = conn_db.cursor()

    redim_package = '.'.join([db_error[6], redim_method])
    redim_call_params = parse_redim_parameter(db_error[8])
    logging.debug('redim_package: %s', redim_package)
    logging.debug('redim_call_params: %s', redim_call_params)

    if redim_method.upper() == 'GRANT_REVOKE_ROLE' and 'P_ROLE_NAME' not in redim_call_params:

      # nutno provolat pres vsechny odebirane role
      sql = """select role_name
            from REDIM_OWNER.REDIM_USER_ROLES
            where username = :username"""

      logging.debug('P_USER_NAME: %s', redim_call_params['P_USER_NAME'])

      cursor.execute(
          sql, {'username': redim_call_params['P_USER_NAME']})

      # vytvoreni parametru P_ROLE_NAME ze vsech dostupnych roli pro daneho uzivatele
      redim_call_params['P_ROLE_NAME'] = ','.join([row[0] for row in cursor.fetchall()])
      logging.debug('P_ROLE_NAME: %s', redim_call_params['P_ROLE_NAME'])

    cursor.callproc(redim_package,
                    keywordParameters=redim_call_params)

    conn_db.commit()
    # cursor.close()
    # conn_db.close()

    # return request id when success
    return db_error[0]

  except cx_Oracle.DatabaseError as exc:
    # pouze vypis chybu a pokracuj
    logging.debug('ERROR db: %s', redim_db)
    logging.debug('exception: %s', exc)

    x = exc.args[0]

     # ORA-01918: user 'EXT90030' does not exist
    # uzivatel jiz neexistuje, takze zaloguj jako OK
    if hasattr(x, 'code') and hasattr(x, 'message') \
            and x.code == 1918 and 'ORA-01918' in x.message:
      return db_error[0]


def main():

  # parser na zjisteni hodnoty nazvu db, jinak vsechny db
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--db", help="database", action="store",
                dest="db")

  args = parser.parse_args()

  oracle = Oracle()

  # ziskam vsechny chyby pro danou db/vsechny db
  redim_errors = oracle.get_redim_errors(args.db)

  for db_error in redim_errors:
    # append to ID success, pokud se call znovu povede provolat
    request_id_success = call_pending_task(db_error)
    logging.info('request_id_success: %s', request_id_success)

    # update task v INFP
    oracle.update_pending_task(request_id_success)

if __name__ == "__main__":
  main()
