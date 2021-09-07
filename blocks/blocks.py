from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException # bitcoinRPC
import mariadb # maria DB
import sys


#(var)==========================================================================#

conn_db = ''
cur = ''
bitcoin_rpc = ''

#(method)=======================================================================#

def connectBitcoinRPC():
    global bitcoin_rpc
    bitcoin_rpc = AuthServiceProxy("http://bitcoinrpc:Cn*dntwk@127.0.0.1:8332")
    

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


def dbSaveAndClose():
    global conn_db
    # save
    conn_db.commit()

    # close
    conn_db.close()


# load block height
def loadNowHeight():
    f = open("/collectData/blocks/height.txt","r")
    height = int(f.readline().rstrip())
    f.close()

    return height


def saveNowHeight(height):
    f = open("/collectData/blocks/height.txt", "r+")
    f.write(str(height))
    f.close()


def collectData():
    global bitcoin_rpc
    
    height = loadNowHeight()
    
    # get best block height
    best_height = bitcoin_rpc.getblockcount()
    
    while(height != best_height):
        
        try:
            hash = bitcoin_rpc.getblockhash(height)
            info = bitcoin_rpc.getblock(hash, 1)
            excuteSQL(info)

            height = height + 1  # increase block height
        
        except Exception as ex:
            print("err : " + str(ex))
            saveNowHeight(height)
            break

    saveNowHeight(height)
    print("blocks : save complete! best height = " + str(height))


def excuteSQL(info):
    global cur
    

    sql = "INSERT INTO BBlock VALUES("+ str(info["height"]) + ",'" + str(info["hash"]) + "'," + str(info["time"]) + "," + str(info["size"]) + "," + str(info["strippedsize"]) + "," + str(info["weight"]) + "," + str(info["version"]) + ",'" + str(info["merkleroot"]) + "'," + str(info["nTx"]) + "," + str(info["mediantime"]) + "," + str(info["nonce"]) + ",'" + str(info["bits"]) + "'," + str(info["difficulty"]) + ",'" + str(info["chainwork"]) + "','" + str(info["previousblockhash"]) + "','" + str(info["nextblockhash"]) + "')"
    cur.execute(sql)


if __name__ == '__main__':

    connectMariaDB()
    connectBitcoinRPC()
    collectData()
    dbSaveAndClose()


