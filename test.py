import csv
from itertools import islice
import numpy as np
import sqlalchemy
import glob
import time
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pickle
from datetime import datetime

import torch
import torch.autograd as autograd
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim

#то же саммое что и в main
Base = declarative_base()
engine = create_engine('sqlite:////home/taruu/Рабочий стол/data_sputnic/Meteor/metiors_new', echo=False)
Session = sessionmaker(bind=engine)
session = Session()


class ticks(Base):
    __tablename__ = 'frame'
    id = Column(Integer, primary_key=True, index=False)
    name = Column(String)
    start = Column(Integer)
    end = Column(Integer)
    data = Column(sqlalchemy.types.BLOB)

    def __init__(self,name, start, end, data):
        self.start = start
        self.name = name
        self.end = end
        self.data = data


#print(session.query(ticks).count())
#Костыль века 3000 ой май гад
i=1
start = time.time()
test = True
while test:
    try:
        test = session.query(ticks).get(i)
        print(test.name,i)
        #вывод и получения данных
        ##print(pickle.loads(test.data))
        i+=1
    except:
        test = None
        print('Кол-во элементов:',i)

print(start-time.time())