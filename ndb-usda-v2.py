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

# USDA Data - Standard Reference
s1 = 'https://ndb.nal.usda.gov/ndb/search/list?maxsteps=6&format=&count=&max=50&sort='
s2 = 'fd_s&fgcd=&manu=&lfacet=&qlookup=&ds=Standard+Reference&qt=&qp=&qa=&qn=&q=&ing=&offset='
s3 = '&order=asc'

sl = [s1 + s2, s3]



bl = [b1 + b2, b3]

def get_urls(l, n):
    urls = list()
    for i in range(0, n + 1):
        ix = str(i * 50)
        url = l[0] + ix + l[1]
        urls.append((url,i))
    return urls

urls_s = get_urls(sl, 176)
urls_b = get_urls(bl, 4505)

 

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
    info = s + " - " + str(ix) + ' / ' + str(n) + ' - avg sp: ' + str(avg_sp)
    print(info)
    
def update_timer_mf(ix):
    n = 4506
    elapsed = time.time() - t0
    avg_sp = elapsed / (ix + 1)
    est_rem = ((n - ix) * avg_sp)
    er_min, er_hrs = int(est_rem / 6)/10 , int(est_rem / 360)/10
    s = 'Est. min/hrs: ' + str(er_min) + ' / '+ str(er_hrs)
    info = s + " - " + str(ix) + ' / ' + str(n) + ' - avg sp: ' + str(avg_sp)
    print(info)
    



def process_sr(x):
    url, ix = str(x[0]), x[1]
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
    update_timer_sr(ix)
    return df
    
def process_mf(x):
    url, ix = str(x[0]), x[1]
    df = pd.read_html(url)[0]
    df.columns = ['Data Source', 'ndb_no', 'Description', 'Manufacturer or Food Group']
    soup = get_soup(str(url))
    q1 = 'Click to view reports for this food'
    all_a  = soup.find_all('a',{'title': [q1]})
    food_urls = nl()
    urls_a  = ['https://ndb.nal.usda.gov' + x.get('href').split('?')[0] for x in all_a]
    if i % 2 == 1:
        food_urls.append(urls_a[i])
    food_ids = [str(x.split('/')[-1]) for x in food_urls]
    df['food_urls'] = pd.Series(food_urls)
    df['food_ids'] = pd.Series(food_ids)
    update_timer_mf(ix)
    return df


t0 = time.time()
standard_dfs = lmap(process_sr, urls_sr) # Approx 5-10 mins

t0 = time.time()
manuf_dfs = lmap(process_mf, urls_mf) # Approx






# Combine into one DF
combined_s_df = pd.concat(all_df_s).reset_index()
combined_b_df = pd.concat(all_df_b).reset_index()


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






def get_food_links(x):
    MO1  = requests.get(str(x[0]))
    index = x[1]
    MO1d = MO1.text ## HTML to Text
    sMO1 = BeautifulSoup(MO1d) ## Input to Beautiful Soup
    foods = sMO1.find_all('a',{'title': "Click to view reports for this food"})
    hrefs = list()
    for link in foods:
        url = str(link.get('href'))
        ix = url.find('?')
        hrefs.append('https://ndb.nal.usda.gov' + url[:ix])
    hrefs_u = list(set(hrefs))
    update_timer(index + 1, 177)
    return '\n'.join(hrefs_u)



test_df = pd.read_html(get_url(t1,t2,0))[0]

test_df

t0 = time.time()
frep_links_s = lmap(get_food_links, urls_s)

s = '\n'.join(frep_links_s)
frep_links_s = s.split('\n')

# Save into DF
links_s_df = pd.Series(frep_links_s).to_frame().reset_index()
links_s_df.columns = ['index', 'report_link']

# Get CSV links
csv_download = '?format=Full&reportfmt=csv&Qv=1'
def get_csv_links(x):
    return str(x) + csv_download

csv_links_s = lmap(get_csv_links, frep_links_s)


s1 = '/ndb/foods/show/9?n1=%7BQv%3D1%7D&fgcd=&man=&lfacet=&count=&max=50&sort=default&qlookup='
s2 = '&offset=&format=Stats&new=&measureby=&ds=&qt=&qp=&qa=&qn=&q=&ing='

stat_links_s = list()
count = 0
for i in range(len(ndb_id_list)):
    nbd_id = ndb_id_list[i]
    stat_link = s1 + str(nbd_id) + s2
    stat_links_s.append(('https://ndb.nal.usda.gov' + stat_link, nbd_id, count))
    count = count + 1
    

stat_to_df = [tpl[0] for tpl in stat_links_s]

# Save data
combined_s_df['frep_link'] = pd.Series(frep_links_s) 
combined_s_df['csv_link']  = pd.Series(csv_links_s) 
combined_s_df['stat_link'] = pd.Series(stat_to_df)

# Update Excel
wr_s = pd.ExcelWriter('usda-sr.xlsx')
combined_s_df.to_excel(wr_s,'Sheet1')
wr_s.save()


def to_excel(df, filename):
    wr_s = pd.ExcelWriter(str(filename) + '.xlsx')
    df.to_excel(wr_s,'Sheet1')
    wr_s.save()


def get_dfs_stat(x):
    return pd.read_html(x)[0]



def update_timer_stat(ix, n):
    elapsed = time.time() - t0
    avg_sp = elapsed / (ix - 2610) + 1
    est_rem = ((n - ix - 2610 + 1) * avg_sp)
    info = str(ix - 2610) + "/" + str(n) + ' - avg sp: ' + str(avg_sp)
    print("Est. rem: " + str(est_rem/60) + " min - " + info)



  





def get_batch(n):
    hd = 100 * (n - 1)
    tl = min(n * 100,len(stat_links_s))
    return stat_links_s[hd:tl]

batches = lmap(get_batch, [i for i in range(1, 89)])

def clean_cells(x):
    return str(x).replace('--','')

def get_stat_df(tpl):
    url    = str(tpl[0])
    nbd_id = tpl[1]
    df     = get_dfs_stat(url)
    n      = len(df['NDB Ref'].values.tolist())
    ids    = [nbd_id for i in range(0,n)]
    # Add NDB ID and clean cells
    df['NDB Ref'] = pd.Series(ids)
    df  = df.applymap(clean_cells)
    ix = tpl[2]
    update_timer_stat(ix + 1, 6178)
    return df

  

   
    
to_join = list()


stp = [min(90*i,8788) for i in range(0,99)]


batches = list()
for i in range(len(stp) - 1):
    b = stat_links_s[stp[i]:stp[i + 1]]
    batches.append(b)
    

#t0 = time.time()
#all_stats_df = lmap(get_stat_df, stat_links_s)
    
# To avoid lost work, and because lists are unable to hold more than
# 90 dataframes per run
    
t0 = time.time()

b0= lmap(get_stat_df, batches[0])

# (...) full code in appendix





dfs = [df0_5, df6_20, df21_40, df41_60, df61_80, df81_98]

df81_98 

dfs[0].columns[0]

to_stats = list()
to_stats.append(dfs[0].copy().drop(['level_0'], axis = 1))  
to_stats.append(dfs[1].copy().drop(['level_0'], axis = 1))  
to_stats.append(dfs[2].copy().drop(['level_0'], axis = 1))  
to_stats.append(dfs[3].copy().drop(['level_0'], axis = 1))  
to_stats.append(dfs[4].copy().drop(['level_0'], axis = 1))  
to_stats.append(dfs[5].copy().drop(['level_0'], axis = 1))  


stats_df = pd.concat(to_stats).reset_index()


def to_excel(df, filename):
    wr_s = pd.ExcelWriter(str(filename) + '.xlsx')
    df.to_excel(wr_s,'Sheet1')
    wr_s.save()



to_excel(to_stats[0], 'usda-stats-sr-' + str(1))

to_excel(to_stats[1], 'usda-stats-sr-' + str(2))

to_excel(to_stats[2], 'usda-stats-sr-' + str(3))

to_excel(to_stats[3], 'usda-stats-sr-' + str(4))

to_excel(to_stats[4], 'usda-stats-sr-' + str(5))

to_excel(to_stats[5], 'usda-stats-sr-' + str(6))

to_excel(stats_df,'usda-stats-sr')


for i in range(len(to_stats)):
    to_excel(to_stats[i], 'usda-stats-sr-' + str(i))



stats_cols = stats_df.columns
stats_cols = [x for x in stats_cols]

cols = list()
for i in rlen(stats_cols):
    cols.append((stats_cols[i], str(i)))
    
    
hd  = stats_cols[2]
ntr = stats_cols[3]
units = stats_cols[4]
id_col = stats_cols[-2]

'''
Simple stats-df design:
    - NBD Ref
    - Name (to-do)
    - Food Group
    - 
'''

all_df_s[0]

std_ref_df = pd.concat(all_df_s)
manuf_b_df = pd.concat(all_df_b)

std_ref_df.columns = ['Unnamed: 0', 'NDB No.', 'Description', 'Food Group']
std_ref_df = std_ref_df.copy().drop(['Unnamed: 0'], axis = 1)

std_ref_df.columns = ['ndb_id', 'Description', 'Food Group']


nbd_l = lmap(lambda x: str(x), std_ref_df['ndb_id'].values.tolist())
std_ref_df['ndb_id'] = pd.Series(nbd_l)


new_stats = stats_df.copy()

to_drop = ['level_0', 'index', 'Data Points', 'Std. Error', 'Min', 'Max', 
           'df', 'LB', 'UB', '# Studies', 'Source', 'Last Modified']

new_stats = new_stats.drop(to_drop, axis = 1)

new_stats.columns = ['Category', 'Nutrient', 'Unit', 'Value (100g)', 'ndb_id']

new_stats

l, lr_on = 'inner', 'ndb_id'

new_stats_df = pd.merge(new_stats, std_ref_df, how=l,left_on=lr_on, right_on=lr_on)




new_stats_df.columns

reorder_cols = ['ndb_id', 'Description', 'Food Group', 'Category', 
                'Nutrient', 'Unit', 'Value (100g)', ]

new_stats_df = new_stats_df[reorder_cols]

# Save data
to_excel(new_stats_df, 'usda-stats-sr-simple')

def get_unique(l):
    return np.unique(l).tolist()

ix = new_stats_df['Food Group'] == get_unique(new_stats_df['Food Group'])[-1]
 
new_stats_df[ix]


# Make new transposed DF
t_df = new_stats_df.copy()[['ndb_id', 'Description', 'Food Group']].drop_duplicates()

hd, ntr, units, id_col, vals = 'Category', 'Nutrient', 'Unit', 'ndb_id', 'Value (100g)'

headings   = new_stats_df[hd].values.tolist()
nutrients  = new_stats_df[ntr].values.tolist()
units_vals = new_stats_df[units].values.tolist()
nbd_ids    = new_stats_df[id_col].values.tolist()



def valid_sep(l, sep):
    return len([s for s in l if sep in s]) == 0

def valid_sep_sc(l):
    return valid_sep(l, ';')

num_vals = len(headings)
val_cols = [headings, nutrients, units_vals]

False in [valid_sep_sc(x) for x in val_cols] # ; is a valid separator

# Category; Nutrient; Unit
cnu_list = nl()
for i in range(num_vals):
    cnu_list.append('; '.join([x[i] for x in val_cols]))



new_stats_df['to_t_df'] = pd.Series(cnu_list)

new_stats_df

dims_unique = np.unique(cnu_list).tolist()

len(dims_unique)



n = len(t_df.ndb_id)
empty_l = ['' for x in range(n)]

for i in rlen(dims_unique):
    new_col = str(dims_unique[i])
    t_df[new_col] = pd.Series(empty_l)


ix1 = new_stats_df['to_t_df'] == dims_unique[0]
ix2 = new_stats_df['ndb_id'] == '2871'

'2871' in nbd_ids


len([s for s in cnu_list if 'Broccoli' in s]) == 0

new_stats_df[ix2]

new_stats_df.copy().T



unique_ids = np.unique(nbd_ids).tolist()
unique_ids # ['IU', 'g', 'kJ', 'kcal', 'mg', 'µg']
len(unique_ids) # 117


len(headings)
len(nutrients)
len(units_vals)

unique_nutrients

nutrients
hnu_list = list()
for i in range(len(headings)):
    h = headings[i] + ' - '
    n = nutrients[i] + ' - '
    u = units_vals[i]
    hnu_list.append(h + n + u)

unique_hnu = np.unique(hnu_list).tolist()
unique_hnu
len(unique_hnu) # 118

stats_df[:150]

stats_df


stats_cols
stats_df[stats_df[id_col] == '11090']



'''
To do's:
    - Eliminate redundant info i.e. kcal only if both exist
    - Normalize to same units i.e. g
    - 

'''



#  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #
#  #  #  #  #  #  #            Appendix            #  #  #  #  #  #  #  #  #  #
#  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #  #



# t0 = time.time()
# all_stats_df = lmap(get_stat_df, stat_links_s)
    
# To avoid lost work, and because lists are unable to hold more than
# 90 dataframes per run

t0 = time.time()
b0= lmap(get_stat_df, batches[0])
b1= lmap(get_stat_df, batches[1])
b2= lmap(get_stat_df, batches[2])
b3= lmap(get_stat_df, batches[3])
b4= lmap(get_stat_df, batches[4])
b5= lmap(get_stat_df, batches[5])


df0 = pd.concat(b0).reset_index()
df1 = pd.concat(b1).reset_index()
df2 = pd.concat(b2).reset_index()
df3 = pd.concat(b3).reset_index()
df4 = pd.concat(b4).reset_index()
df5 = pd.concat(b5).reset_index()


['df' + str(i) for i in range(0,6)]

to_join = [df0, df1, df2, df3, df4, df5]

df0_5 = pd.concat(to_join).reset_index()

del(df0)
del(df1)
del(df2)
del(df3)
del(df4)
del(df5)




b6= lmap(get_stat_df, batches[6])
b7= lmap(get_stat_df, batches[7])
b8= lmap(get_stat_df, batches[8])
b9= lmap(get_stat_df, batches[9])
b10= lmap(get_stat_df, batches[10])
b11= lmap(get_stat_df, batches[11])
b12= lmap(get_stat_df, batches[12])
b13= lmap(get_stat_df, batches[13])
b14= lmap(get_stat_df, batches[14])
b15= lmap(get_stat_df, batches[15])
b16= lmap(get_stat_df, batches[16])
b17= lmap(get_stat_df, batches[17])
b18= lmap(get_stat_df, batches[18])
b19= lmap(get_stat_df, batches[19])
b20= lmap(get_stat_df, batches[20])

df6 = pd.concat(b6).reset_index()
df7 = pd.concat(b7).reset_index()
df8 = pd.concat(b8).reset_index()
df9 = pd.concat(b9).reset_index()
df10 = pd.concat(b10).reset_index()
df11 = pd.concat(b11).reset_index()
df12 = pd.concat(b12).reset_index()
df13 = pd.concat(b13).reset_index()
df14 = pd.concat(b14).reset_index()
df15 = pd.concat(b15).reset_index()
df16 = pd.concat(b16).reset_index()
df17 = pd.concat(b17).reset_index()
df18 = pd.concat(b18).reset_index()
df19 = pd.concat(b19).reset_index()
df20 = pd.concat(b20).reset_index()


['df' + str(i) for i in range(6,21)]

to_join = [df6, df7, df8, df9, df10, df11, df12, df13, df14, df15, df16, df17, df18, df19, df20]
df6_20 = pd.concat(to_join).reset_index()




b21= lmap(get_stat_df, batches[21])
b22= lmap(get_stat_df, batches[22])
b23= lmap(get_stat_df, batches[23])
b24= lmap(get_stat_df, batches[24])
b25= lmap(get_stat_df, batches[25])
b26= lmap(get_stat_df, batches[26])
b27= lmap(get_stat_df, batches[27])
b28= lmap(get_stat_df, batches[28])


# Below this line: to-do
t0 = time.time()

b29= lmap(get_stat_df, batches[29])
b30= lmap(get_stat_df, batches[30])
b31= lmap(get_stat_df, batches[31])
b32= lmap(get_stat_df, batches[32])
b33= lmap(get_stat_df, batches[33])
b34= lmap(get_stat_df, batches[34])
b35= lmap(get_stat_df, batches[35])
b36= lmap(get_stat_df, batches[36])
b37= lmap(get_stat_df, batches[37])
b38= lmap(get_stat_df, batches[38])
b39= lmap(get_stat_df, batches[39])
b40= lmap(get_stat_df, batches[40])


df21 = pd.concat(b21).reset_index()
df22 = pd.concat(b22).reset_index()
df23 = pd.concat(b23).reset_index()
df24 = pd.concat(b24).reset_index()
df25 = pd.concat(b25).reset_index()
df26 = pd.concat(b26).reset_index()
df27 = pd.concat(b27).reset_index()
df28 = pd.concat(b28).reset_index()
df29 = pd.concat(b29).reset_index()
df30 = pd.concat(b30).reset_index()
df31 = pd.concat(b31).reset_index()
df32 = pd.concat(b32).reset_index()
df33 = pd.concat(b33).reset_index()
df34 = pd.concat(b34).reset_index()
df35 = pd.concat(b35).reset_index()
df36 = pd.concat(b36).reset_index()
df37 = pd.concat(b37).reset_index()
df38 = pd.concat(b38).reset_index()
df39 = pd.concat(b39).reset_index()
df40 = pd.concat(b40).reset_index()



', '.join(['df' + str(i) for i in range(20,41)])
to_join = [df21, df22, df23, df24, df25, df26, df27, df28, df29, df30, df31, df32, df33, df34, df35, df36, df37, df38, df39, df40]
df21_40 = pd.concat(to_join).reset_index()


b41= lmap(get_stat_df, batches[41])
b42= lmap(get_stat_df, batches[42])
b43= lmap(get_stat_df, batches[43])
b44= lmap(get_stat_df, batches[44])
b45= lmap(get_stat_df, batches[45])
b46= lmap(get_stat_df, batches[46])
b47= lmap(get_stat_df, batches[47])
b48= lmap(get_stat_df, batches[48])
b49= lmap(get_stat_df, batches[49])
b50= lmap(get_stat_df, batches[50])
b51= lmap(get_stat_df, batches[51])
b52= lmap(get_stat_df, batches[52])
b53= lmap(get_stat_df, batches[53])
b54= lmap(get_stat_df, batches[54])
b55= lmap(get_stat_df, batches[55])
b56= lmap(get_stat_df, batches[56])
b57= lmap(get_stat_df, batches[57])
b58= lmap(get_stat_df, batches[58])
b59= lmap(get_stat_df, batches[59])
b60= lmap(get_stat_df, batches[60])

df41 = pd.concat(b41).reset_index()
df42 = pd.concat(b42).reset_index()
df43 = pd.concat(b43).reset_index()
df44 = pd.concat(b44).reset_index()
df45 = pd.concat(b45).reset_index()
df46 = pd.concat(b46).reset_index()
df47 = pd.concat(b47).reset_index()
df48 = pd.concat(b48).reset_index()
df49 = pd.concat(b49).reset_index()
df50 = pd.concat(b50).reset_index()
df51 = pd.concat(b51).reset_index()
df52 = pd.concat(b52).reset_index()
df53 = pd.concat(b53).reset_index()
df54 = pd.concat(b54).reset_index()
df55 = pd.concat(b55).reset_index()
df56 = pd.concat(b56).reset_index()
df57 = pd.concat(b57).reset_index()
df58 = pd.concat(b58).reset_index()
df59 = pd.concat(b59).reset_index()
df60 = pd.concat(b60).reset_index()

', '.join(['df' + str(i) for i in range(40,61)])
to_join = [df41, df42, df43, df44, df45, df46, df47, df48, df49, df50, df51, df52, df53, df54, df55, df56, df57, df58, df59, df60]
df41_60 = pd.concat(to_join).reset_index()


b61= lmap(get_stat_df, batches[61])
b62= lmap(get_stat_df, batches[62])
b63= lmap(get_stat_df, batches[63])
b64= lmap(get_stat_df, batches[64])
b65= lmap(get_stat_df, batches[65])
b66= lmap(get_stat_df, batches[66])
b67= lmap(get_stat_df, batches[67])
b68= lmap(get_stat_df, batches[68])
b69= lmap(get_stat_df, batches[69])
b70= lmap(get_stat_df, batches[70])
b71= lmap(get_stat_df, batches[71])
b72= lmap(get_stat_df, batches[72])
b73= lmap(get_stat_df, batches[73])
b74= lmap(get_stat_df, batches[74])
b75= lmap(get_stat_df, batches[75])
b76= lmap(get_stat_df, batches[76])
b77= lmap(get_stat_df, batches[77])
b78= lmap(get_stat_df, batches[78])
b79= lmap(get_stat_df, batches[79])
b80= lmap(get_stat_df, batches[80])


df61 = pd.concat(b61).reset_index()
df62 = pd.concat(b62).reset_index()
df63 = pd.concat(b63).reset_index()
df64 = pd.concat(b64).reset_index()
df65 = pd.concat(b65).reset_index()
df66 = pd.concat(b66).reset_index()
df67 = pd.concat(b67).reset_index()
df68 = pd.concat(b68).reset_index()
df69 = pd.concat(b69).reset_index()
df70 = pd.concat(b70).reset_index()
df71 = pd.concat(b71).reset_index()
df72 = pd.concat(b72).reset_index()
df73 = pd.concat(b73).reset_index()
df74 = pd.concat(b74).reset_index()
df75 = pd.concat(b75).reset_index()
df76 = pd.concat(b76).reset_index()
df77 = pd.concat(b77).reset_index()
df78 = pd.concat(b78).reset_index()
df79 = pd.concat(b79).reset_index()
df80 = pd.concat(b80).reset_index()


', '.join(['df' + str(i) for i in range(61,81)])
to_join = [df61, df62, df63, df64, df65, df66, df67, df68, df69, df70, df71, df72, df73, df74, df75, df76, df77, df78, df79, df80]
df61_80 = pd.concat(to_join).reset_index()

b81= lmap(get_stat_df, batches[81])
b82= lmap(get_stat_df, batches[82])
b83= lmap(get_stat_df, batches[83])
b84= lmap(get_stat_df, batches[84])
b85= lmap(get_stat_df, batches[85])
b86= lmap(get_stat_df, batches[86])
b87= lmap(get_stat_df, batches[87])
b88= lmap(get_stat_df, batches[88])
b89= lmap(get_stat_df, batches[89])
b90= lmap(get_stat_df, batches[90])
b91= lmap(get_stat_df, batches[91])
b92= lmap(get_stat_df, batches[92])
b93= lmap(get_stat_df, batches[93])
b94= lmap(get_stat_df, batches[94])
b95= lmap(get_stat_df, batches[95])
b96= lmap(get_stat_df, batches[96])
b97= lmap(get_stat_df, batches[97])
b98= lmap(get_stat_df, batches[98])



df81 = pd.concat(b81).reset_index()
df82 = pd.concat(b82).reset_index()
df83 = pd.concat(b83).reset_index()
df84 = pd.concat(b84).reset_index()
df85 = pd.concat(b85).reset_index()
df86 = pd.concat(b86).reset_index()
df87 = pd.concat(b87).reset_index()
df88 = pd.concat(b88).reset_index()
df89 = pd.concat(b89).reset_index()
df90 = pd.concat(b90).reset_index()
df91 = pd.concat(b91).reset_index()
df92 = pd.concat(b92).reset_index()
df93 = pd.concat(b93).reset_index()
df94 = pd.concat(b94).reset_index()
df95 = pd.concat(b95).reset_index()
df96 = pd.concat(b96).reset_index()
df97 = pd.concat(b97).reset_index()
df98 = pd.concat(b98).reset_index()


', '.join(['df' + str(i) for i in range(81,99)])

to_join = [df81, df82, df83, df84, df85, df86, df87, df88, df89, df90, df91, df92, df93, df94, df95, df96, df97, df98]
df81_98 = pd.concat(to_join).reset_index()






## Ignore
to_soup = list()
for i in range(len(frep_links_s)):
    to_soup.append((frep_links_s[i],i))

t0 = time.time()
def get_soups(x):
    MO1  = requests.get(str(x[0]))
    index = x[1]
    MO1d = MO1.text ## HTML to Text
    sMO1 = BeautifulSoup(MO1d) ## Input to Beautiful Soup
    update_timer(index + 1, len(frep_links_s))
    return sMO1


frep_soups = lmap(get_soups, to_soup)


