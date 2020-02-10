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
import torch
import torch.autograd as autograd
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim


Base = declarative_base()

#Импорт баззы данных
engine = create_engine('sqlite:////home/taruu/Рабочий стол/data_sputnic/AURORA', echo=False)

#Сессия для запросов
Session = sessionmaker(bind=engine)
session = Session()

#Папка с txt
#TODO читаем xz ибо шакалы
floader = "/home/taruu/Рабочий стол/data_sputnic/met_AURORA"



#Струкура бд которую мы юзаем
class ticks(Base):
    #Название таблицы в бд
    __tablename__ = 'frame'
    #Id это индекс
    id = Column(Integer, primary_key=True)
    #Название это название
    name = Column(String)
    #начание unixtime
    start = Column(Integer)
    #unixtime end
    end = Column(Integer)
    #храним данные в двоичном формате
    data = Column(sqlalchemy.types.BLOB)
    #разметка для питоня
    def __init__(self,name, start, end, data):
        self.start = start
        self.name = name
        self.end = end
        self.data = data




#Загружаем и считаваем данные
def load_file(filename):
    frames_x16 = [] # массив по 16*16 *256
    lines = []
    with open(filename) as inp:
        for line in list(islice(inp, 256)):
            l = [x for x in (' '.join(line.split())).split(' ')]
            lines.append(l)

    for j in range(2, 258):
        frame = []
        for k in range(16):
            row = []
            for l in range(16):
                row.append(int(lines[16 * k + l][j]))
            frame.append(row)
        frames_x16.append(frame)

    frames_x256 = [] #16*256 *16 0_o
    for row in range(16):
        col_list = []
        for col in range(256):
            col_list.append(frames_x16[col][0])
        frames_x256.append(col_list)


    return frames_x16,frames_x256, max([np.max(a) for a in frames_x16])




def data_in_bd(files):
    need_add =[]
    #перебераем список файлов и работаем с ним и добавляем
    for file in files:
        frames_x16_in,frames_x256_in,test2 = load_file(floader + "/" + file)
        start = file.split("-")[1] #unixtime start
        end = file.split("-")[2]   #unixtime end
        startdatatime = datetime.strptime(start, "%y%m%d_%H%M%S").timestamp()
        enddatatime = datetime.strptime(end, "%y%m%d_%H%M%S").timestamp()

        #сама конвертация данных и суем их в словарь
        #pickle.dumps делает нам байтовый код чтоб в бд хранить
        data = pickle.dumps({"frames_x16": torch.Tensor(frames_x16_in), "frames_x256": torch.Tensor(frames_x256_in)})


        need_add.append(ticks(file, startdatatime, enddatatime, data))
    #добавить все сразу
    session.add_all(need_add)
    #добавить в бд или закомитить в бд
    session.commit()


#data_in_bd("sdsd")

#print(os.listdir(floader) )

#Поможет если вы застопили прогу но не прогнали все до конца то можно ввести номер
ine = int(input("На чем остоновились:"))

list_files = []
#перебераем все файлы с расширнием txt
#TODO на xz арзхивы пм
for i,item in enumerate(glob.glob(floader+"/*.txt")):
    #сотню отсчитали и отправили на запись
    if len(list_files) == 100:
        print("Опять работа")
        data_in_bd(list_files)
        list_files = []

    if ine < i:
        list_files.append(item.split("/")[-1])

        print(i, item)
    else:
        print(i,"not need add")
else:
    if list_files:
        data_in_bd(list_files)