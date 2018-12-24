#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 16 14:35:26 2018

@author: ye
"""

import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData, Column, Integer, String, Float, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session,sessionmaker
import json
import pandas as pd

with open('config5.json') as f:
    db_URI = json.load(f)

engine = create_engine(db_URI.get('URI'))
metadata = MetaData()

DBsession = sessionmaker(bind=engine)
session = DBsession()
Base = declarative_base()


class Revenue(Base):
    __tablename__ = 'kewl_mediasource_daily_monitor_v5'
    id = Column(Integer, primary_key=True)
    country = Column(String)
    media_source = Column(String)
    platform = Column(String)
    spend = Column(Float)
    install = Column(Integer)
    all_income3 = Column(Float)
    remain1 = Column(Integer)


st = session.query(Revenue).filter(Revenue.media_source == 'applovin_int').limit(5)

# Table('census', metadata, autoload=True, autoload_with=engine)
# for i in st:
#     print(i.id, i.country)
df = pd.read_sql_query(st, engine)
print(df)










