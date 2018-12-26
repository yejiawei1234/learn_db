#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 16 14:35:26 2018

@author: ye
"""

from sqlalchemy import create_engine, MetaData, select, Table, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import and_, or_, between, case, cast
import json
import pandas as pd
from datetime import date
import datetime

with open('config5.json') as f:
    db_URI = json.load(f)

engine = create_engine(db_URI.get('URI'))
metadata = MetaData()
connection = engine.connect()
Base = declarative_base()

# area type
tw_area = ('TW', 'HK', 'MO', 'SG', 'MY', 'CN')
level1 = ('SA', 'AE', 'QA')
level2 = ('JO', 'BH', 'LB', 'OM', 'KW', 'EG')
level3 = ('MA', 'DZ')
gb_area = ('UK', 'AU', 'NZ', 'GB')

country_list = ('AE', 'AU', 'BH', 'BR', 'CA', 'CN', 'DE', 'DZ', 'EG', 'ES', 'FI', 'FR', 'HK', 'ID',
                'IE', 'IN', 'IQ', 'IT', 'JO', 'JP', 'KW', 'LB', 'LT', 'LY', 'MA', 'MO', 'MR', 'MX',
                'MY', 'NG', 'NL', 'NZ', 'OM', 'PK', 'PL', 'QA', 'RU', 'SA', 'SG', 'TH', 'TN', 'TR',
                'TW', 'UK', 'US', 'VN', 'GB', 'PS')

# media_source type
google = {
    'googleadwords_int', 'googleadwordsoptimizer_int', 'wezonet', 'wezonet3', 'meitu_300', 'hinamob', 'adtime',
    'imygbs2'}

kewl_rev = Table('kewl_mediasource_daily_monitor_v5', metadata, autoload=True, autoload_with=engine)


today = date.today()
month_ago = today - datetime.timedelta(days=31)




country_ = case(
    [
        (kewl_rev.c.country == 'US', 'A_美国'),
        (kewl_rev.c.country.in_(tw_area), 'A_台湾'),
        (kewl_rev.c.country == 'IN', 'A_印度'),
        (kewl_rev.c.country == 'BR', 'A_巴西'),
        (kewl_rev.c.country.in_(level1), 'level1'),
        (kewl_rev.c.country.in_(level2), 'level2'),
        (kewl_rev.c.country.in_(level3), 'level3'),
        (kewl_rev.c.country.in_(gb_area), '英联邦区')
    ], else_='A_其他'
)

channel_ = case(
    [
        (kewl_rev.c.media_source.in_(google), 'Google'),
        (kewl_rev.c.media_source == 'Facebook Ads', 'Facebook'),
        (kewl_rev.c.media_source == 'Twitter', 'Twitter'),
        (kewl_rev.c.media_source == 'Apple Search Ads', 'ASM'),
        (kewl_rev.c.media_source == 'unknown', '未知渠道'),
        (kewl_rev.c.media_source == 'Apple Search Ads', 'ASM'),
    ], else_='网盟'
)

platform_ = case(
    [
        (kewl_rev.c.platform == 'ios', 'IOS'),
        (kewl_rev.c.platform == 'Android', 'Android'),
    ], else_='Unknown'
)

zi = ('imygbs2', 'unknown')
partner_ = case(
    [
        (kewl_rev.c.partner.in_(zi), '自投')
    ], else_='代投'
)

stmt = select(
    [
        kewl_rev.c.date,
        country_.label('area'),
        channel_.label('channel'),
        platform_,
        partner_.label('is_zitou'),
        func.sum(kewl_rev.c.install).label('install'),
        func.sum(kewl_rev.c.all_income3).label('all_income3'),
        func.sum(kewl_rev.c.all_income7).label('all_income7'),
        func.sum(kewl_rev.c.all_income7).label('all_income7'),
        func.sum(kewl_rev.c.all_income15).label('all_income15'),
        func.sum(kewl_rev.c.all_income30).label('all_income30'),
        func.sum(kewl_rev.c.remain1).label('remain1'),
        func.sum(kewl_rev.c.remain3).label('remain3'),
        func.sum(kewl_rev.c.remain7).label('remain7'),
        func.sum(kewl_rev.c.income3_uv).label('income3_uv'),
        func.sum(kewl_rev.c.login).label('login'),

    ]
)

filter_the_all = and_(
    kewl_rev.c.partner != 'all',
    kewl_rev.c.media_source != 'all',
    kewl_rev.c.platform != 'all',
    kewl_rev.c.country.in_(country_list),
    kewl_rev.c.date >= month_ago
)

# group_by_list =


stmt = stmt.where(filter_the_all).group_by(kewl_rev.c.date,
                                           'area',
                                           'channel',
                                           kewl_rev.c.platform,
                                           'is_zitou').order_by(kewl_rev.c.date.desc()).limit(20)

results = connection.execute(stmt).fetchall()
df = pd.DataFrame(results)  # 这里就把results转变为了df
df.columns = results[0].keys()

print(df)
engine.dispose()
