#!/usr/bin/env python3

"""
https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/index.html
"""

from os import environ

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy import update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.oracle import TIMESTAMP

Base = declarative_base()

# pripojeni k repository INFP pres DASHBOARD
DSN_INFP = 'oracle+cx_oracle://dashboard:abcd1234@oem12.vs.csin.cz:1521/?service_name=INFP'

ORACLE_ENV_VARIABLE = {
    'ORACLE_HOME': '/oracle/product/db/19',
    'LD_LIBRARY_PATH': '/oracle/product/db/19/lib',
    'TNS_ADMIN_DIR': '/etc/oracle/wallet/system',
    'NLS_LANG': 'AMERICAN_AMERICA.AL32UTF8'
}

# cx_Oracle variables ENV
for key, val in ORACLE_ENV_VARIABLE.items():
  if key.upper() not in os.environ:
    os.environ[key.upper()] = val


# SQLAlchemy Declarative Mapping
class RedimDatabases(Base):
  __tablename__ = 'redim_databases'
  __table_args__ = {'schema': 'REDIM_OWNER'}
  database = Column(String, primary_key=True)
  hostname = Column(String)
  port = Column(Integer)
  redim_username = Column(String)
  redim_password = Column(String)
  Session = sessionmaker(bind=engine, autocommit=True)
  session = Session()


# seznam tasku ke znovuspusteni
redim_errors = session.query(RedimPendingTask) \
    .filter(RedimPendingTask.status == 'ERROR') \
    .all()


## cx_Oracle metoda
# class pro pripojeni k Oracle db INFP
class OracleINFP(object):
  """public class for Connect to Oracle"""

  def __init__(self):
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
    self.cursor.execute(sql, {'id': request_id})
    self.db.commit()
