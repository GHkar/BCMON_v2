from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException # bitcoinRPC
import mariadb # maria DB
import sys
import time

#(var)==========================================================================#

conn_db = ''
cur = ''
bitcoin_rpc = ''

#(method)=======================================================================#

def connectBitcoinRPC():
    global bitcoin_rpc
    bitcoin_rpc = AuthServiceProxy("http://bitcoinrpc:Cn*dntwk@127.0.0.1:8333")


def connectMariaDB():
    global conn_db
    global cur


    # connect MariaDB
    conn_db = mariadb.connect(
            user="root",
            password="Cn*dntwk",
            host="169.254.31.40",
            port=3406,
            database="Bitcoin"
            )

    # create cursor
    cur = conn_db.cursor() 


def dbSave():
    global conn_db
    # save
    conn_db.commit()

def dbClose():
    # close
    global conn_db
    conn_db.close()


# load block height
def loadNowHeight():
    f = open("/height4.txt","r")
    height = int(f.readline().rstrip())
    f.close()

    return height


def saveNowHeight(height):
    f = open("/height4.txt", "r+")
    f.write(str(height))
    f.close()


def collectData():
    global bitcoin_rpc

    height = loadNowHeight()

    # get best block height
    best_height = 400000#bitcoin_rpc.getblockcount()
    
    while(height != best_height):
        try:
            # get hash and get block info
            hash = bitcoin_rpc.getblockhash(height)
            block = bitcoin_rpc.getblock(hash, 2)
            transaction = block["tx"]   # tx list
            first = True    # is that coinbase?
            
            # get tx info
            txindex = 0      # input index of tx / where is it in the block?
            for tx in transaction :
                executeSQL_tx(tx, txindex, block["height"], block["hash"], block["time"])      # input transaction
                index = 0       # input index of vin / where is it in the transaction? 

                if first :
                    executeSQL_cb(block["height"], txindex, tx["vin"][0], index, tx["hash"])
                    first = False
                else :
                    for vi in tx["vin"] :
                        executeSQL_vin(block["height"], txindex, vi, index, tx["hash"])
                        index = index + 1
                
                for vo in tx["vout"] :
                    # if there is no address
                    if "addresses" in vo["scriptPubKey"]:
                        executeSQL_vout(block["height"], txindex, vo, tx["hash"])
                    else : 
                        executeSQL_vout_no_ad(block["height"], txindex, vo, tx["hash"])

                dbSave()        # successfully input one transaction's info than commit (= save)
                txindex = txindex + 1
            height = height + 1  # increase block height

        except Exception as ex:
            
            if 'pipe' in str(ex):
                # print("broken pipe error")
                saveNowHeight(height)
                dbClose()
                connectMariaDB()
                connectBitcoinRPC()
                height = loadNowHeight() 
                best_height = bitcoin_rpc.getblockcount()

                continue
            
            elif 'Remote' in str(ex):
                print("Remote connection error")
                saveNowHeight(height)
                dbClose()
                
                time.sleep(5)
                connectMariaDB()
                connectBitcoinRPC()
                height = loadNowHeight()
                best_height = bitcoin_rpc.getblockcount()

                continue

            else :
                print("err : " + str(ex))
                saveNowHeight(height)
                break

        # checking data
        if height % 1000 == 0 :
            print("saving data, now height = " + str(height))

    saveNowHeight(height)
    print("transaction : save complete! best height = " + str(height))


def executeSQL_tx(info, txindex, blockheight, blockhash, blocktime):
    global cur  
    
    try :
        # input BTransaction
        sql = "INSERT INTO BTransaction2 VALUES("+ str(blockheight) + "," + str(txindex) + ",'" + str(info["txid"]) + "','" + str(blockhash) + "'," + str(blocktime)+ ",'" + str(info["hash"]) + "'," + str(info["size"]) + "," + str(info["vsize"]) + "," + str(info["weight"]) + "," + str(info["version"]) + "," + str(info["locktime"]) +")"
        cur.execute(sql)
    # there is duplicate data, so replace it
    except Exception as ex:
        if 'Duplicate' in str(ex):
            print("Duplicate TX : " + str(info["txid"]) + ", Block : " + str(blockhash))
            sql = "DELETE FROM BVin2 WHERE TransactionID='" + str(info["txid"]) + "'"

            cur.execute(sql)

            sql = "DELETE FROM BVout2 WHERE TransactionID='" + str(info["txid"]) + "'"

            cur.execute(sql)

            sql = "REPLACE INTO BTransaction2 VALUES("+ str(blockheight) + "," + str(txindex) + ",'" + str(info["txid"]) + "','" + str(blockhash) + "'," + str(blocktime)+ ",'" + str(info["hash"]) + "'," + str(info["size"]) + "," + str(info["vsize"]) + "," + str(info["weight"]) + "," + str(info["version"]) + "," + str(info["locktime"]) +")"
        
            cur.execute(sql)


def executeSQL_vin(blockheight, txindex, info, index, txhash):
   # input BVin
    global cur
    sql = "INSERT INTO BVin2(Height,TransactionIndex,TransactionID,nIndex,UTXOID,vout) VALUES(" + str(blockheight) + "," + str(txindex) + ",'" + str(txhash) + "'," + str(index) + ",'" + str(info["txid"]) + "'," + str(info["vout"]) + ")"
    
    cur.execute(sql)

def executeSQL_cb(blockheight, txindex, info, index, txhash):
    # input coinbase
    global cur
    sql = "INSERT INTO BVin2(Height,TransactionIndex,TransactionID,nIndex,Coinbase) VALUES(" + str(blockheight) + "," + str(txindex) + ",'" + str(txhash) + "'," + str(index) + ",'" + str(info["coinbase"]) + "')"

    cur.execute(sql)


def executeSQL_vout(blockheight, txindex, info, txhash):
    # input BVout
    sql = "INSERT INTO BVout2 VALUES(" + str(blockheight) + "," + str(txindex) + ",'"+ str(txhash) + "'," + str(info["n"]) + "," + str(info["value"])+",'"+ str(info["scriptPubKey"]["addresses"][0]) + "','" + str(info["scriptPubKey"]["type"]) + "')" 

    cur.execute(sql)

def executeSQL_vout_no_ad(blockheight, txindex, info, txhash):
    # input BVout
    sql = "INSERT INTO BVout2(Height, TransactionIndex, TransactionID, n, value, type) VALUES(" + str(blockheight) + "," + str(txindex) + ",'"+ str(txhash) + "'," + str(info["n"]) + "," + str(info["value"])+",'" + str(info["scriptPubKey"]["type"]) + "')"

    cur.execute(sql)



if __name__ == '__main__':

    connectMariaDB()
    connectBitcoinRPC()
    collectData()
    dbClose()
