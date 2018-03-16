#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  5 16:22:14 2018

@author: juanmanubens
"""

import requests
import pandas as pd
import numpy as np
import time
import sys
import re
from lxml import html
from lxml import etree
from bs4 import BeautifulSoup
from time import sleep
from datetime import datetime
from functools import reduce

def lmap(fx, l):
    return list(map(fx, l))

def rlen(x):
    return range(len(x))

def nl():
    return list()


# USDA Data - Standard Reference
def get_url_sr(ix):
    t1 = 'https://ndb.nal.usda.gov/ndb/search/list?maxsteps=6&format=&count=&max=50'  
    t2 = '&sort=fd_s&fgcd=&manu=&lfacet=&qlookup=&ds=Standard+Reference'
    t3, num, t4 = '&qt=&qp=&qa=&qn=&q=&ing=&offset=', str(ix*50), '&order=asc'
    return t1 + t2 + t3 + num + t4


# USDA Data - Manufacturer
def get_url_mf(ix):
    t1 = 'https://ndb.nal.usda.gov/ndb/search/list?format=&count=&max=50&sort='  
    t2 = 'fd_s&fgcd=&manu=&lfacet=&qlookup=&ds=&qt=&qp=&qa=&qn=&q=&ing=&offset='
    num, t3 = str(ix*50), '&order=asc'
    return t1 + t2 + num + t3


urls_sr = [(get_url_sr(x),x) for x in range(0, 176)]
urls_mf = [(get_url_mf(x),x) for x in range(0, 4506)]



def get_soup(x):
    MO1  = requests.get(str(x))
    MO1d = MO1.text ## HTML to Text
    return BeautifulSoup(MO1d, "lxml") ## Input to Beautiful Soup

def update_timer_sr(ix):
    n = 177
    elapsed = time.time() - t0
    avg_sp = elapsed / (ix + 1)
    est_rem = ((n - ix) * avg_sp)
    er_min, er_hrs = int(est_rem / 6)/10 , int(est_rem / 360)/10
    s = 'Est. min/hrs: ' + str(er_min) + ' / '+ str(er_hrs)
    sp = str(int(100 * avg_sp)/100) + ' - Elapsed: ' + str(elapsed/60) + ' mins'
    info = s + " - " + str(ix) + ' / ' + str(n) + ' - avg sp: ' + sp 
    print(info)
    
def update_timer_mf(ix):
    n = 4506
    elapsed = time.time() - t0
    avg_sp = elapsed / (ix + 1)
    est_rem = ((n - ix) * avg_sp)
    er_min, er_hrs = int(est_rem / 6)/10 , int(est_rem / 360)/10
    s = 'Est. min/hrs: ' + str(er_min) + ' / '+ str(er_hrs)
    sp = str(int(100 * avg_sp)/100) + ' - Elapsed: ' + str(elapsed/60) + ' mins'
    info = s + " - " + str(ix) + ' / ' + str(n) + ' - avg sp: ' + sp 
    print(info)
    


def process_usda(x):
    url = str(x[0])
    df = pd.read_html(url)[0]
    df.columns = ['Data Source', 'ndb_no', 'Description', 'Manufacturer or Food Group']
    soup = get_soup(str(url))
    q1 = 'Click to view reports for this food'
    all_a = soup.find_all('a',{'title': [q1]})
    food_urls = nl()
    urls_a = ['https://ndb.nal.usda.gov' + x.get('href').split('?')[0] for x in all_a]
    for i in rlen(urls_a):
        if i % 2 == 1:
            food_urls.append(urls_a[i])
    food_ids = [str(x.split('/')[-1]) for x in food_urls]
    s1 = '?n1=%7BQv%3D1%7D&fgcd=&man=&lfacet=&count=&max=50&sort=fd_s&qlookup=&offset=0&'
    s2 = 'format=Stats&new=&measureby=&ds=Standard+Reference&qt=&qp=&qa=&qn=&q=&ing='
    stat_urls = [str(x) + s1 + s2 for x in food_urls]
    df['food_urls'] = pd.Series(food_urls)
    df['food_ids'] = pd.Series(food_ids)
    df['stat_urls'] = pd.Series(stat_urls)
    return df

def process_sr(x):
    df, ix = process_usda(x), x[1]
    update_timer_sr(ix)
    return df
    
def process_mf(x):
    df, ix = process_usda(x), x[1]
    update_timer_mf(ix)
    return df



t0 = time.time()
standard_dfs = lmap(process_sr, urls_sr) # Approx 5-10 mins


# Join Std
all_sr_df = pd.concat(standard_dfs).reset_index()

sr_stat_urls = all_sr_df.stat_urls.values.tolist()
nbd_no  = all_sr_df.ndb_no.values.tolist()
food_no = all_sr_df.food_ids.values.tolist()



sl = [sr_stat_urls, nbd_no, food_no]
to_stat_processing = nl()

for i in range(len(sr_stat_urls)):
    tpl = (sl[0][i], sl[1][i], sl[2][i], i)
    to_stat_processing.append(tpl)



def get_dfs_stat(x):
    df_stat = pd.read_html(x[0])[0]
    nbd_no, food_no = x[1], x[2]
    df_stat['NDB Ref']  = pd.Series([nbd_no for i in range(len(df_stat))])
    df_stat['food_no'] = pd.Series([food_no for i in range(len(df_stat))])
    update_timer_stat(x[-1])
    return df_stat

def update_timer_stat(ix):
    n = 8789
    elapsed = time.time() - t0
    avg_sp = elapsed / (ix + 1)
    est_rem = ((n - ix) * avg_sp)
    er_min, er_hrs = int(est_rem / 6)/10 , int(est_rem / 360)/10
    s = 'Est. min/hrs: ' + str(er_min) + ' / '+ str(er_hrs)
    sp = str(int(100 * avg_sp)/100) + ' - Elapsed: ' + str(elapsed/60) + ' mins'
    info = s + " - " + str(ix) + ' / ' + str(n) + ' - avg sp: ' + sp 
    print(info)


def to_excel(df, filename):
    wr_s = pd.ExcelWriter(str(filename) + '.xlsx')
    df.to_excel(wr_s,'Sheet1')
    wr_s.save()

t0 = time.time()
df0 = get_dfs_stat(to_stat_processing[0])

latest = 0


from urllib.error import HTTPError
    
t0 = time.time()
while latest < 8780:
    try:
        for i in range(latest, 8789):
            l_df = [df0]
            l_df.append(get_dfs_stat( to_stat_processing[i]))
            df0 = pd.concat(l_df)
            latest = i
    except HTTPError:
        print('\n' + 'Pausing for 2 seconds - n = ' + str(latest))
        time.sleep(2)
        
        
#t0 = time.time()
#df_mf0 = process_mf(urls_mf[0])
#latest_mf = 1

df0_clean = df0.copy().drop_duplicates()


def clean_cells(x):
    return str(x).replace('--','')

df0_clean = df0_clean.applymap(clean_cells)


df0_clean

sr_cols = df0_clean.columns.values.tolist()


sr_cols[:3]

 


to_excel(df0_clean, 'usda-stats-sr-backup-' + str(1))

ndb_no

n_no, f_id, f_url = all_sr_df.ndb_no , all_sr_df.food_ids, all_sr_df.food_urls

n_no, f_id, f_url = n_no.values.tolist(), f_id.values.tolist(), f_url.values.tolist()

fni = nl()
for i in rlen(n_no):
    to_list = (f_url[i], n_no[i], f_id[i])
    fni.append(to_list)

len(fni)

food_urls_sr = get_unique(all_sr_df.food_urls)


food_urls_sr



t0 = time.time()
while latest_mf < 4509:
    for i in range(latest_mf, 4509):
        try:
            l_mf_df = [df_mf0]
            l_mf_df.append(process_mf(urls_mf[i]))
            df_mf0 = pd.concat(l_mf_df)
            latest_mf = i
        except HTTPError:
            print('\n' + 'Pausing for 2 seconds - n = ' + str(latest_mf))
            time.sleep(2)
            

df_mf0_clean = df_mf0.copy().drop_duplicates()

df_mf0_clean = df_mf0_clean.reset_index()


# Join Branded Products
all_mf_df = df_mf0_clean


def len_u(x):
    return len(get_unique(x))


to_xls = all_mf_df['ndb_no']

to_xls['stat_urls'] = pd.Series(all_mf_df.stat_urls.values.tolist())

to_xls.stat_urls

to_excel(to_xls, 'usda-mf-stat_urls')


all_mf_df

mf_stat_urls = all_mf_df.stat_urls.values.tolist()
nbd_no_mf  = all_mf_df.ndb_no.values.tolist()
food_no_mf = all_mf_df.food_ids.values.tolist()



sl_mf = [mf_stat_urls, nbd_no_mf, food_no_mf]
to_stat_proc_mf = nl()

for i in range(len(mf_stat_urls)):
    tpl = (sl_mf[0][i], sl_mf[1][i], sl_mf[2][i], i)
    to_stat_proc_mf.append(tpl)


lim = len(all_mf_df)

lim #225270

def update_timer_stat_mf(ix):
    n = 225270
    elapsed = int(100*(time.time() - t0))/100
    avg_sp = elapsed / (ix + 1)
    est_rem = ((n - ix) * avg_sp)
    er_min, er_hrs = int(est_rem / 6)/10 , int(est_rem / 360)/10
    s = 'Est. min/hrs: ' + str(er_min) + ' / '+ str(er_hrs)
    e_min = int(100*(elapsed/60))/100
    sp = str(int(100 * avg_sp)/100) + ' - Elapsed: ' + str(e_min) + ' mins'
    perc_n = (10000*ix) / n
    p = int(perc_n) / 100
    info = s + " - " + str(p) + '%' + ' - avg sp: ' + sp 
    if ix % 50 == 0:
        print(info)
    
    

def get_dfs_stat_mf(x):
    df_stat = pd.read_html(x[0])[0]
    nbd_no, food_no = x[1], x[2]
    df_stat['NDB Ref']  = pd.Series([nbd_no for i in range(len(df_stat))])
    df_stat['food_no'] = pd.Series([food_no for i in range(len(df_stat))])
    update_timer_stat_mf(x[-1])
    return df_stat



from urllib.error import HTTPError

from urllib.error import URLError

   
df_mf_stat0 = get_dfs_stat(to_stat_proc_mf[0])
latest_mf_stat = 1


df_mf_stat0.columns


t0 = time.time()
update_timer_stat_mf(100)


while latest_mf_stat < 225270:
    try:
        for i in range(latest_mf_stat, 225270):
            l_df_mf = [df_mf_stat0]
            l_df_mf.append(get_dfs_stat_mf(to_stat_proc_mf[i]))
            df_mf_stat0 = pd.concat(l_df_mf)
            latest_mf_stat = i
    except HTTPError:
        e  = 'HTTPError: '
        print('\n' + e + 'Pausing for 2 seconds - n = ' + str(latest_mf_stat))
        time.sleep(2)
    except URLError:
        e  = 'URLError: '
        print('\n' + e + 'Pausing for 2 seconds - n = ' + str(latest_mf_stat))
        time.sleep(2)
    except OSError:
        e  = 'OSError: '
        print('\n' + e + 'Pausing for 2 seconds - n = ' + str(latest_mf_stat))
        time.sleep(2)



latest_mf_stat


df_mf_stat0.copy().drop_duplicates()


df_mf_stat0

# Save into files
wr_s = pd.ExcelWriter('usda-sr.xlsx')
combined_s_df.to_excel(wr_s,'Sheet1')
wr_s.save()


wr_b = pd.ExcelWriter('usda-mf.xlsx')
combined_b_df.to_excel(wr_b,'Sheet1')
wr_b.save()



# Next level
combined_s_df.columns  = ['index', 'ignore', 'ndb_id', 'desc', 'food_group']

ndb_id_list = combined_s_df['ndb_id'].values.tolist()








