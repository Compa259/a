import sqlalchemy.types as T
from sqlalchemy import Column

from common.database.base import Base


class DomainCityInfo(Base):
    __tablename__ = 'domain_city_info'
    id = Column(T.Integer(), primary_key=True)
    city_id = Column(T.Integer())
    domain_id = Column(T.Integer())
    city_name = Column(T.String())
    value = Column(T.Integer())
    id_region_in_domain = Column(T.String())
    root_province_id = Column(T.Integer())


class DomainCityInfo(Base):
    __tablename__ = 'domain_city_info_pro'
    id = Column(T.Integer(), primary_key=True)
    city_id = Column(T.Integer())
    domain_id = Column(T.Integer())
    city_name = Column(T.String())
    value = Column(T.Integer())
    id_region_in_domain = Column(T.String())
    root_province_id = Column(T.Integer())