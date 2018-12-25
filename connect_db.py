#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 16 14:35:26 2018

@author: ye
"""

import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData, Column, Integer, String, Float, select, Table, case, literal
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session,sessionmaker
import json
import pandas as pd

with open('config5.json') as f:
    db_URI = json.load(f)

engine = create_engine(db_URI.get('URI'))
metadata = MetaData()
connection = engine.connect()
Base = declarative_base()


# class Revenue(Base):
#     __tablename__ = 'kewl_mediasource_daily_monitor_v5'
#     id = Column(Integer, primary_key=True)
#     country = Column(String)
#     media_source = Column(String)
#     platform = Column(String)
#     spend = Column(Float)
#     install = Column(Integer)
#     all_income3 = Column(Float)
#     remain1 = Column(Integer)


# st = session.query(Revenue).filter(Revenue.media_source == 'applovin_int').limit(5)


tw_area = ('TW', 'HK', 'MO', 'SG', 'MY', 'CN')
tw_area = literal(tw_area)
level1 = ('SA', 'AE', 'QA')
level1 = literal(level1)
level2 = ('JO', 'BH', 'LB', 'OM', 'KW', 'EG')
level2 = literal(level2)
level3 = ('MA', 'DZ')
level3 = literal(level3)
gb_area = ('UK', 'AU', 'NZ', 'GB')
gb_area = literal(gb_area)

kewl_rev = Table('kewl_mediasource_daily_monitor_v5', metadata, autoload=True, autoload_with=engine)

country_ = case(
    [
        (kewl_rev.c.country == 'US', 'A_美国'),
        (kewl_rev.c.country == tw_area, 'A_台湾'),
        (kewl_rev.c.country == 'IN', 'A_印度'),
        (kewl_rev.c.country == 'BR', 'A_巴西'),
        (kewl_rev.c.country == level1, 'level1'),
        (kewl_rev.c.country == level2, 'level2'),
        (kewl_rev.c.country == level3, 'level3'),
        (kewl_rev.c.country == gb_area, '英联邦区')
    ], else_='A_其他'
)

stmt = select(
    [
        kewl_rev.c.media_source,
        kewl_rev.c.install,
        kewl_rev.c.spend,

        # country_.label('area')
    ]
).limit(20)

results = connection.execute(stmt).fetchall()
df = pd.DataFrame(results) #这里就把results转变为了df
df.columns = results[0].keys()

print(df)
engine.dispose()









