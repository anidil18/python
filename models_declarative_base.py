# models.py

"""
http://flask.pocoo.org/docs/0.12/patterns/sqlalchemy/
"""

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class CloningRelation(Base):
  __tablename__ = 'CLONING_TARGET_DATABASE'
  __table_args__ = {'schema': 'CLONING_OWNER'}

  target_licdb_id = Column(Integer, primary_key=True)
  target_dbname = Column(String(100), unique=True)
  target_hostnames = Column(String(4000))
  source_dbname = Column(String(100))
  method_name = Column(String(30))
  template_name = Column(String(100))

  def __repr__(self):
    return '<target_dbname: %r>' % (self.target_dbname)

  def to_json(self):
    return dict(id=self.target_licdb_id, target_dbname=self.target_dbname)
