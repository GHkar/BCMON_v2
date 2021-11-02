from web3 import Web3, HTTPProvider, IPCProvider # ethereumRPC
# import pymongo
# from pymongo import MongoClient
import mariadb # maria DB
import json
import sys
import time
from hexbytes import HexBytes # hex code 해석


#(var)==========================================================================#

# conn_db = ''    # db connect
# cur = ''        # maria db insert command

# db_json = {}

# Ethereum Node RPC Connection
eth_rpc = Web3(HTTPProvider("http://localhost:8545"))

#(class)========================================================================#

# hex code encoding
class HexJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, HexBytes):
            return str(obj.hex())

#(method)=======================================================================#


# maria DB
def connectMariaDB():
    global conn_db
    global cur

    # connect MariaDB
    conn_db = mariadb.connect(
            user="root",
            password="Cn*dntwk",
            host="169.254.31.40",
            port=3406,
            database="Ethereum"
            )
    
    # create cursor
    cur = conn_db.cursor() 


def dbSave():
    global conn_db

    conn_db.commit()

def dbClose():
    global conn_db

    conn_db.close()


def excuteSQL(info):
    global cur
    
    #sql = "INSERT INTO ETransaction VALUES("+ str(info["blockNumber"]) + ",'" + str(info["blockHash"]) + "'," + str(info["transactionIndex"]) + ",'" + str(info["hash"]) + "','" + str(info["from"]) + "'," + str(info["gas"]) + "," + str(info["gasPrice"]) + ",'" + str(info["input"]) + "'," + str(info["nonce"]) + ",'" + str(info["r"]) + "','" + str(info["s"]) + "','" + str(info["to"]) + "','" + str(info["type"]) + "'," + str(info["v"]) + "," + str(info["value"]) + ")"

    sql = "INSERT INTO ETransaction VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    
    cur.executemany(sql, info)


# load block height
def loadNowHeight():
    f = open("/home/ethereum/collectData/transaction/height.txt","r")
    height = int(f.readline().rstrip())
    f.close()

    return height


def saveNowHeight(height):
    f = open("/home/ethereum/collectData/transaction/height.txt", "r+")
    f.write(str(height))
    f.close()

def makeTuple(info) :

    tup = (str(info["blockNumber"]), str(info["blockHash"]), str(info["transactionIndex"]), str(info["hash"]), str(info["from"]), str(info["gas"]), str(info["gasPrice"]), str(info["input"]), str(info["nonce"]), str(info["r"]), str(info["s"]), str(info["to"]), str(info["type"]), str(info["v"]), str(info["value"]))

    return tup

def collectData():
    global eth_rpc
    
    height = loadNowHeight()
    
    # get best block height
    best_block = eth_rpc.eth.getBlock('latest')
    best_height = best_block['number']
    
    while(height != best_height):
        txlist = []
        try:
            blockTx_raw = dict(eth_rpc.eth.getBlock(height, True))  # AttributeDict -> Dict   //True 처리해주면 트랜잭션 데이터까지 같이 출력

            for tx_raw in blockTx_raw['transactions'] :
                tx_dict = dict(tx_raw)                                  # AttributeDict -> Dict
                tx_dump = json.dumps(tx_dict, cls=HexJsonEncoder)       # hex code encoding json
                tx = json.loads(tx_dump)                                # decode json
                txlist.append(makeTuple(tx))

            if len(txlist) != 0:
                excuteSQL(txlist)
                dbSave()

            height = height + 1  # increase block height
        
        except Exception as ex:
            print("err : " + str(ex))
            saveNowHeight(height)
            break
    
    saveNowHeight(height)
    print("blocks : save complete! best height = " + str(height))


if __name__ == '__main__':

    connectMariaDB()
    collectData()
    dbSave()
    dbClose()

