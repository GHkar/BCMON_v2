import pymongo
from pymongo import MongoClient
import json
import re
import time
import datetime

#(var)==========================================================================#

pattern_time = re.compile(r"(?P<date>\d+[-]\d+[-]\d+)T(?P<time>\d+[:]\d+[:]\d+[.]\d+)Z\s")

#(method)=======================================================================#


# connecting mongo raw data base
def connect_monogoDB():
    myclient = pymongo.MongoClient("mongodb://210.125.29.227:27017/")
    mydb = myclient['Bitcoin']
    collection = mydb["Debug_log"]

    reset_nowPos(collection)
    
    return collection

# save reading file pos
def save_nowPos(pos, line):
    f = open("/collectData/log/nowPos.txt", "r+")
    f.write(str(pos) + "\n")
    f.write(str(line))
    f.close()

# load reading file pos
def load_nowPos():    
    f = open("/collectData/log/nowPos.txt", "r")
    readPos = int(f.readline().rstrip())
    line = int(f.readline().rstrip())
    f.close()

    return readPos, line

# if there are no documents in collection, than init pos to zero(0)
def reset_nowPos(collection):
    if collection.estimated_document_count() == 0:      # counting number of document in collection
        f = open("/collectData/log/nowPos.txt", "w+")
        f.write("0\n")
        f.write("1")
        f.close()

# printing result
def print_result(count):
        print("log : Total " + str(count-1) + ", saved at DB.")

# change type string to datetime
def change_dateTime(date, time):
    dateTime = datetime.datetime.strptime(date+" "+time, '%Y-%m-%d %H:%M:%S.%f')
    
    return dateTime

# save data at mongo db
def save_mongo_db(collection, json):

    collection.insert_one(json)


# collecting log data
def collectData(colletion):
    count = 0  # how many input document
    
    f = open("/root/.bitcoin/debug.log", "r") # open log file
    readPos, count = load_nowPos()
    
    if count == 1:
        f.seek(5)  # general open log file, ommit first space
    else:
        f.seek(readPos)     # already open it, move pos

    while True:
        line = f.readline()
        line = line.rstrip()
        json = {}

        if not line:
            nowPos = f.tell()

            break

        try:
            findtime = pattern_time.search(line)
            dt = change_dateTime(findtime.group('date'), findtime.group('time'))
            

            json['_id'] = count
            json['datetime'] = dt
            json['log'] = line

            save_mongo_db(collection, json) 
            count = count + 1

        except Exception as ex:
            print("err : " + str(ex))
            f.close()
            break
        
    save_nowPos(nowPos, count)
    print_result(count)
    f.close()


if __name__ == '__main__':
    collection = connect_monogoDB()
    #collectData(collection)

    
