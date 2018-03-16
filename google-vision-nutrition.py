#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  8 18:54:58 2018

@author: juanmanubens
"""



import pandas as pd
import numpy as np
import requests, time, json, sys, io, os, re
from lxml import html
from lxml import etree
from bs4 import BeautifulSoup
from time import sleep
from datetime import datetime
from functools import reduce
from urllib.error import HTTPError


import google
from google import cloud

# Imports the Google Cloud client library
from google.cloud import vision
from google.cloud.vision import types



def detect_text(path):
    """Detects text in the file."""
    client = vision.ImageAnnotatorClient()

    with io.open(path, 'rb') as image_file:
        content = image_file.read()

    image = types.Image(content=content)

    response = client.text_detection(image=image)
    texts = response.text_annotations
    #print(texts)
    print('Texts:')

    for text in texts:
        a=("{}".format(text.description))
        print(a)
        #vertices = (['({},{})'.format(vertex.x, vertex.y)
        #            for vertex in text.bounding_poly.vertices])

        #print('bounds: {}'.format(','.join(vertices)))'''
    print((a))


#give path to your Service account keys .json file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '' 
testimg = 'gvt.png'

   					  #give path to your image
detect_text(testimg)


'''
Texts:
Nutrition Facts
Serv.Size 1 tbsp (15mL)
Servings Per Container about 63
Amount Per Serving
Calories 120Fat Cal. 120
Total Fat 14g
% Daily Value*
22%
10%
Saturated Fat2g
Trans Fat 0g
Polyunsat. Fat 1g
ーー
Monounsat. Fat 11g
Sodium Omg
Total Carb.0g
Protein 0g
source of cholesterot, eta
A, vitamin C calum
are based ona2000
INGREDIENTS EXTRA IR
VIRGIN OLIVE OL

Nutrition
Facts
Serv.Size
1
tbsp
(15mL)
Servings
Per
Container
about
63
Amount
Per
Serving
Calories
120Fat
Cal.
120
Total
Fat
14g
%
Daily
Value
*
22
%
10
%
Saturated
Fat2g
Trans
Fat
0g
Polyunsat.
Fat
1g
ーー
Monounsat.
Fat
11g
Sodium
Omg
Total
Carb.0g
Protein
0g
source
of
cholesterot,
eta
A,
vitamin
C
calum
are
based
ona2000
INGREDIENTS
EXTRA
IR
VIRGIN
OLIVE
OL
OL
'''