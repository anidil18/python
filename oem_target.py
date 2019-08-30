#!/usr/bin/env python

"""
Oracle Enterprise Manager
- custom dynamic inventory script for Ansible

Develop mode - cx_oracle nahrazen za sqlite
CREATE TABLE mgmt_targets(target_name TEXT, target_type TEXT)
INSERT INTO mgmt_targets VALUES ('dordb01','host')
INSERT INTO mgmt_targets VALUES ('tordb01','host')

"""

__version__ = '1.2'
__author__ = 'Jiri Srba'
__email__ = 'jsrba@csas.cz'
__status__ = 'Development'


import sys
import os
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import pprint


# Global variables
DSN_TNS = 'dashboard:abcd1234@oem12-m.vs.csin.cz:1521/INFP'
# DSN_TNS = 'dashboard:abcd1234@oem12.vs.csin.cz:1521/INFP'

SQLALCHEMY_DATABASE_URI = 'oracle://' + DSN_TNS

# need to set this, or Oracle won't retrieve utf8 chars properly
os.environ["NLS_LANG"] = 'AMERICAN_AMERICA.AL32UTF8'

# SQLAlchemy declarative base
Base = declarative_base()
engine = create_engine(SQLALCHEMY_DATABASE_URI)
session = scoped_session(sessionmaker(autocommit=False, autoflush=False,
                                      bind=engine))


class MgmtTarget(Base):
  __tablename__ = 'MGMT$TARGET'
  __table_args__ = {'schema': 'DASHBOARD'}

  target_guid = Column(String, primary_key=True)
  target_name = Column(String(256), unique=True)
  target_type = Column(String(64))
  host_name   = Column(String(256))

  def __repr__(self):
    return '<target_name: %r>' % (self.target_name)


def print_result(result):
  """Print list results nicely """

  pp = pprint.PrettyPrinter(indent=4)
  pp.pprint(result)

  # print dict/list generated from query
  # print json.dumps({'database': hosts }, sort_keys=True, indent=4)

  #  for result in query:
  #      print json.dumps(result)


def get_db_target(dbname):
  """List all OEM target starting with target_name
  """

  """
  SELECT
    target_name, target_type, host_name
  FROM
    MGMT$TARGET
  WHERE
    target_type IN ('oracle_database','rac_database')
      AND TARGET_NAME like :1
  """

  query = session.\
      query(MgmtTarget.target_name, MgmtTarget.target_type, MgmtTarget.host_name).\
      filter(MgmtTarget.target_name.like(dbname)).\
      filter(MgmtTarget.target_type.in_(['oracle_database', 'rac_database'])).\
      order_by(MgmtTarget.target_name)

  target = query.all()

  print_result(target)


def get_group_members(host):
  """Get group names from host
  """

  """
    SELECT
    AGGREGATE_TARGET_NAME "GROUP",
    member_target_name "SERVER"
  FROM
    MGMT$TARGET_FLAT_MEMBERS
  WHERE
    MEMBER_TARGET_TYPE  IN ('host')
    AND MEMBER_TARGET_NAME like 'dordb04%';
  """

  # hosts = query_oem(sql)


def main(args):

  dbname = ''.join(args)

  if len(sys.argv) > 1:
    dbname = ' '.join(args)
  else:
    dbname = 'JIRKA'

  get_db_target(dbname)

  # get_init_params(dbname)


if __name__ == "__main__":
  main(sys.argv[1:])
