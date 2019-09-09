#!/usr/bin/env python3


""" Retry Redim ERROR tasks """

from __future__ import unicode_literals
from __future__ import print_function

import argparse
import logging
import os
import logging
import cx_Oracle
import numpy as np
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy import update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.oracle import TIMESTAMP

__version__ = '2019-09-09'
__author__ = 'Jiri Srba'
__email__ = 'jsrba@csas.cz'
__status__ = 'Development'

Base = declarative_base()

# pripojeni k repository INFP pres DASHBOARD
DSN_INFP = 'oracle+cx_oracle://dashboard:abcd1234@oem12.vs.csin.cz:1521/?service_name=INFP'

ORACLE_ENV_VARIABLE = {
    'ORACLE_HOME': '/oracle/product/db/19',
    'LD_LIBRARY_PATH': '/oracle/product/db/19/lib',
    'TNS_ADMIN_DIR': '/etc/oracle/wallet/system',
    'NLS_LANG': 'AMERICAN_AMERICA.AL32UTF8'
}

# set logging
logging.basicConfig(level=logging.DEBUG)

# SQLAlchemy Declarative Mapping
class RedimDatabases(Base):
  __tablename__ = 'redim_databases'
  __table_args__ = {'schema': 'REDIM_OWNER'}
  database = Column(String, primary_key=True)
  hostname = Column(String)
  port = Column(Integer)
  redim_username = Column(String)
  redim_password = Column(String)


class RedimPendingTask(Base):
  __tablename__ = 'redim_pending_task'
  __table_args__ = {'schema': 'REDIM_OWNER'}
  request_id = Column(Integer, primary_key=True)
  database = Column(String)
  redim_package = Column(String)
  task_name = Column(String)
  task_params = Column(String)
  status = Column(String)
  last_update_date = Column(DateTime)


def parse_redim_parameter(param):
  """ Konverze Redim parametru do dict fomratu
      P_USER_NAME=cen85437,P_OPERATION_TYPE=0,P_DB_NAME=CRMDB"""

  param_dict = dict((x.strip(), y.strip()) for x, y in (element.split('=')
                                                  for element in param.split(',')))
  return param_dict


def call_pending_task(db_error, conn_string):

  redim_method = db_error.task_name

  redim_package = '.'.join([db_error.redim_package, redim_method])
  redim_call_params = parse_redim_parameter(db_error.task_params)
  logging.debug('redim_package: %s', redim_package)
  logging.debug('redim_call_params: %s', redim_call_params)

  # znovu zopakuj TASK
  try:
    logging.debug('conn_string: %s', conn_string)

    conn_db = cx_Oracle.connect(**conn_string)

    cursor = conn_db.cursor()

    # GRANT_REVOKE_ROLE - nutno provolat pres vsechny dostupne role
    if redim_method.upper() == 'GRANT_REVOKE_ROLE' and 'P_ROLE_NAME' not in redim_call_params:

      sql = """select role_name
            from REDIM_OWNER.REDIM_USER_ROLES
            where username = :username"""

      logging.debug('username: %s', redim_call_params['P_USER_NAME'])

      cursor.execute(
          sql, {'username': redim_call_params['P_USER_NAME']})

      # revoke vsech ziskanych roli
      for role in cursor.fetchall():
        redim_call_params['P_ROLE_NAME'] = role[0]
        logging.debug('revoke role: %s', redim_call_params['P_ROLE_NAME'])
        cursor.callproc(redim_package, keywordParameters=redim_call_params)

    else:
      # proved obecne volani REDIM_DB_USERS
      cursor.callproc(redim_package, keywordParameters=redim_call_params)

    # commit;
    conn_db.commit()

    # return request id when success
    return True

  except cx_Oracle.DatabaseError as exc:
    # pouze vypis chybu a pokracuj
    logging.debug('ERROR db: %s', db_error.database)
    logging.debug('exception: %s', exc)

    x = exc.args[0]

    # ORA-01918: user 'EXT90030' does not exist -> zaloguj status jako OK
    if hasattr(x, 'code') and hasattr(x, 'message') \
            and x.code == 1918 and 'ORA-01918' in x.message:
      return True

    # retry se nepovedl, vrat False
    return False


def main():

  # parser na zjisteni hodnoty nazvu db, jinak vsechny db
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--db", help="database", action="store",
                dest="db")

  args = parser.parse_args()

  # cx_Oracle ENV
  for key, val in ORACLE_ENV_VARIABLE.items():
    if key.upper() not in os.environ:
      os.environ[key.upper()] = val

  # SQLAlchemy
  engine = create_engine(DSN_INFP)

  Session = sessionmaker(bind=engine, autocommit=True)
  session = Session()

  # seznam tasku ke znovuspusteni
  # bez .all(), aby to slo pouzit pro pandas
  redim_errors = session.query(RedimPendingTask) \
      .filter(RedimPendingTask.status == 'ERROR')

  # ziskam vsechny chyby pro danou db/vsechny db
  for db_error in redim_errors.all():

    redim_database = session.query(RedimDatabases) \
        .filter(RedimDatabases.database == db_error.database) \
        .one_or_none()

    # vytvoreni connect stringu pro pripojeni na remote db
    dsn = cx_Oracle.makedsn(
        redim_database.hostname, redim_database.port,
        service_name=db_error.database)

    conn_string = dict(
        user=redim_database.redim_username,
        password=redim_database.redim_password,
        dsn=dsn
        )

    # request_id_success = True / False
    request_id_success = call_pending_task(db_error, conn_string)

    # pokud se povede task zopakovat zmen status na OK
    #
    if request_id_success:

      # oracle.update_pending_task(request_id_success)
      db_error.status = 'OK'
      db_error.last_update_date = func.now()
      logging.info('database: %s OK', db_error.database)

    else:
      logging.info('database: %s ERROR', db_error.database)

  df = pd.read_sql(redim_errors.statement, redim_errors.session.bind)
  if not df.empty:
    print(df)

  # vse na konci commituj - nastaven misto toho autocommit
  # session.commit()
  session.close()


if __name__ == "__main__":
  main()
