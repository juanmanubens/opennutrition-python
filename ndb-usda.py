#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  5 16:22:14 2018

@author: juanmanubens
"""

import requests
import pandas as pd
import time
import sys
import re
from lxml import html
from lxml import etree
from bs4 import BeautifulSoup
from time import sleep
from datetime import datetime
from functools import reduce



# USDA Data
s1 = "https://ndb.nal.usda.gov/ndb/search/list?format=&count=&max=50&sort="
s2 = "fd_s&fgcd=&manu=&lfacet=&qlookup=&ds=&qt=&qp=&qa=&qn=&q=&ing=&offset="
s3 = "&order=asc"

urls  = list()
for i in range(0,4506):
    ix = str(i*50)
    url = s1 + s2 + ix + s3
    urls.append(  (url,i) )
  

def lmap(fx, l):
    return list(map(fx, l))


t0 = time.time()

def update_timer(ix):
    elapsed = time.time() - t0
    avg_sp = elapsed / ix + 1
    est_rem = ((4606 - ix + 1) * avg_sp)
    print("Time remaining:" + str(est_rem) + " s, " + str(est_rem/60) + " min")
    

# Scrape with Pandas
def get_dfs(x):
    ix = x[1] + 1
    update_timer(int(ix))
    return pd.read_html(x[0])[0]

t0 = time.time()
all_df = lmap(get_dfs, urls)

# Combine into one DF
combined_df = pd.concat(all_df).reset_index()
combined_df = combined_df



wr = pd.ExcelWriter('usda.xlsx')
combined_df.to_excel(wr,'Sheet1')
wr.save()





