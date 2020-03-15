import csv
from itertools import islice
import numpy as np
import sqlalchemy
import glob
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pickle
from datetime import datetime
import os
import time
import torch
import torch.autograd as autograd
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim


database = "/home/taruu/Рабочий стол/data_sputnic/met2"
txt_path = "/home/taruu/data_satellite/txt/metiors"


Base = declarative_base()
engine = create_engine('sqlite:///' + database, echo=False)
Session = sessionmaker(bind=engine)
session = Session()

class frame(Base):
    __tablename__ = 'frame'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    start = Column(Integer,index=True)
    end = Column(Integer)
    lat = Column(Integer,index=True)
    lon = Column(Integer,index=True)
    height = Column(Integer,index=True)
    def __init__(self, name, start, end, lat, lon, height):
        self.start = start
        self.name = name
        self.end = end
        self.lat = lat
        self.lon = lon
        self.height = height

class data(Base):
    id = Column(Integer, primary_key=True, index=True)
    data = Column(sqlalchemy.types.BLOB)
    def __init__(self, id, data):
        self.id = id
        self.data = data

class hv_line(Base):
    id = Column(Integer, primary_key=True, index=True)
    hv1 = Column(Integer, index=True)
    hv2 = Column(Integer, index=True)
    hv3 = Column(Integer, index=True)
    hv4 = Column(Integer, index=True)
    hv5 = Column(Integer, index=True)
    hv6 = Column(Integer, index=True)
    hv7 = Column(Integer, index=True)
    hv8 = Column(Integer, index=True)
    hv9 = Column(Integer, index=True)
    hv10 = Column(Integer, index=True)
    hv11 = Column(Integer, index=True)
    hv12 = Column(Integer, index=True)
    hv13 = Column(Integer, index=True)
    hv14 = Column(Integer, index=True)
    hv15 = Column(Integer, index=True)
    hv16 = Column(Integer, index=True)
    hv17 = Column(Integer, index=True)
    hv18 = Column(Integer, index=True)
    hv20 = Column(Integer, index=True)
    hv21 = Column(Integer, index=True)
    hv22 = Column(Integer, index=True)
    hv23 = Column(Integer, index=True)
    hv24 = Column(Integer, index=True)
    hv25 = Column(Integer, index=True)
    hv26 = Column(Integer, index=True)
    hv27 = Column(Integer, index=True)
    hv28 = Column(Integer, index=True)
    hv29 = Column(Integer, index=True)
    hv30 = Column(Integer, index=True)
    hv31 = Column(Integer, index=True)
    hv32 = Column(Integer, index=True)
    def __init__(self, id, list_in):
        self.id = id
        self.hv1 = list_in[0]
        self.hv2 = list_in[1]
        self.hv3 = list_in[2]
        self.hv4 = list_in[3]
        self.hv5 = list_in[4]
        self.hv6 = list_in[5]
        self.hv7 = list_in[6]
        self.hv8 = list_in[7]
        self.hv9 = list_in[8]
        self.hv10 = list_in[9]
        self.hv11 = list_in[10]
        self.hv12 = list_in[11]
        self.hv13 = list_in[12]
        self.hv14 = list_in[13]
        self.hv15 = list_in[14]
        self.hv16 = list_in[15]
        self.hv17 = list_in[16]
        self.hv18 = list_in[17]
        self.hv19 = list_in[18]
        self.hv20 = list_in[19]
        self.hv21 = list_in[20]
        self.hv22 = list_in[21]
        self.hv23 = list_in[22]
        self.hv24 = list_in[23]
        self.hv25 = list_in[24]
        self.hv26 = list_in[25]
        self.hv27 = list_in[26]
        self.hv28 = list_in[27]
        self.hv29 = list_in[28]
        self.hv30 = list_in[29]
        self.hv31 = list_in[30]
        self.hv32 = list_in[31]

