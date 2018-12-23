#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 16 14:35:26 2018

@author: ye
"""

import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session,sessionmaker
import json

with open('config5.json') as f:
    db_URI = json.load(f)

print(db_URI.get('URI'))
#engine = create_engine(db_URI)
# metadata = MetaData()
#
# DBsession = sessionmaker(bind=engine)
# session = DBsession()
# Base = declarative_base()
#table_i_can_read = [i for i in engine.table_names() if i.startswith('kewl')]
#engine.dispose()







