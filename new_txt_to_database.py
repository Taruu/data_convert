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
import lzma
import os
import time
import torch
import torch.autograd as autograd
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim


database = "/home/taruu/data_satellite/database/ofther"
txt_path = "/home/taruu/data_satellite/txt/ofther"


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
    def __init__(self,id, name, start, end, lat, lon, height):
        self.id = id
        self.name = name
        self.start = start
        self.end = end
        self.lat = lat
        self.lon = lon
        self.height = height

class data_table(Base):
    __tablename__ = 'data'
    id = Column(Integer, primary_key=True, index=True)
    data = Column(sqlalchemy.types.BLOB)
    def __init__(self, id, data):
        self.id = id
        self.data = data

class hv_line(Base):
    __tablename__ = 'hv_line'
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
    hv19 = Column(Integer, index=True)
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


def load_file_xz(filename):
    start = filename.split("-")[1]
    end = filename.split("-")[2]
    startdatatime = datetime.strptime(start, "%y%m%d_%H%M%S").timestamp()
    enddatatime = datetime.strptime(end, "%y%m%d_%H%M%S").timestamp()
    frames_x16 = []  # массив по 16*16 *256
    lines = []
    with lzma.open(filename,"rt") as inp:
        for line in list(islice(inp, 2560)):
            l = [x for x in (' '.join(line.split())).split(' ')]
            lines.append(l)

    with lzma.open(filename,"rt") as inp:
        list_hv = next(islice(inp, 256, 257)).split()
        list_hv.pop(0)
        list_hv.pop(0)

    with lzma.open(filename,"rt") as inp:
        LLA_coordinates = next(islice(inp, 268, 269)).split()
        LLA_coordinates.pop(0)
        LLA_coordinates.pop(0)

    for j in range(2, 258):
        frame = []
        for k in range(16):
            row = []
            for l in range(16):
                row.append(int(lines[16 * k + l][j]))
            frame.append(row)
        frames_x16.append(frame)

    return {"frames_x16": frames_x16, "lsit_hv": list_hv, "LLA_coordinates": LLA_coordinates, "start": startdatatime,
            "end": enddatatime}

def load_file_txt(filename):
    start = filename.split("-")[1]
    end = filename.split("-")[2]
    startdatatime = datetime.strptime(start, "%y%m%d_%H%M%S").timestamp()
    enddatatime = datetime.strptime(end, "%y%m%d_%H%M%S").timestamp()
    frames_x16 = [] # массив по 16*16 *256
    lines = []
    with open(filename) as inp:
        for line in list(islice(inp, 2560)):
            l = [x for x in (' '.join(line.split())).split(' ')]
            lines.append(l)


    with open(filename) as inp:
        list_hv = next(islice(inp, 256, 257)).split()
        list_hv.pop(0)
        list_hv.pop(0)

    with open(filename) as inp:
        LLA_coordinates = next(islice(inp, 268, 269)).split()
        LLA_coordinates.pop(0)
        LLA_coordinates.pop(0)


    for j in range(2, 258):
        frame = []
        for k in range(16):
            row = []
            for l in range(16):
                row.append(int(lines[16 * k + l][j]))
            frame.append(row)
        frames_x16.append(frame)
    #
    # frames_x256 = [] #16*256 *16 0_o
    # for row in range(16):
    #     col_list = []
    #     for col in range(256):
    #         col_list.append(frames_x16[col][0])
    #     frames_x256.append(col_list)

    max([np.max(a) for a in frames_x16])
    return {"frames_x16":frames_x16,"lsit_hv":list_hv,"LLA_coordinates":LLA_coordinates,"start":startdatatime,"end":enddatatime}

def insert_data(id,filename,data_dict):
    list_all_data = []
    list_all_data.append(frame(id,filename.split("/")[-1],data_dict["start"],data_dict["end"],data_dict["LLA_coordinates"][0],data_dict["LLA_coordinates"][1],data_dict["LLA_coordinates"][2]))
    list_all_data.append(data_table(id,pickle.dumps(torch.Tensor(data_dict["frames_x16"]))))
    list_all_data.append(hv_line(id,data_dict["lsit_hv"]))
    return list_all_data


add_list = []
print(database)
print(txt_path)
list_files = glob.glob(txt_path+"/*.txt")
list_xz = glob.glob(txt_path+"/*/*")
if len(list_xz) != 0:
    list_files.extend(list_xz)

def take_convert(id,filename):
    if filename.split(".")[-1] == "xz":
        data_all = load_file_xz(file)
    else:
        data_all = load_file_txt(file)
    return insert_data(id,file,data_all)


for id,file in enumerate(list_files):
    #print(id)
    if id < session.query(frame).count():
        continue
    if (id % 100 == 0) and (id != 0):
        print(id, len(add_list),add_list)
        session.add_all(add_list)
        session.commit()
        add_list.clear()
        add_list.extend(take_convert(id, file))

    else:
        add_list.extend(take_convert(id,file))
else:
    print(id, len(add_list), add_list)
    session.add_all(add_list)
    session.commit()

