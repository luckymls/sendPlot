from fileSplit import Filesplit
import os,math


# ----
CLIENT_MODE = 0
SERVER_MODE = 1

MOD = None

FARM_IP = ""
FARM_PORT = 5001
MAX_CONNECTIONS_ALLOWED = 6
# ----

def chooseMod():
    global MOD
    c = False
    
    while not c:
        r = input("Scegli la modalità: \n[0] Client Mode\n[1] Server Mode")

        if r.isdigit() and int(r) in (CLIENT_MODE,SERVER_MODE):
            MOD = int(r)
            c = True
        else:
            print("Modalità non esistente. Riprova.")

   

# -------- SEND FILE --------

# -------- SPLIT & MERGE FILE --------

def merge_cb(f, s):
    #print("File: {0}, Dimensione: {1}".format(f, s))
    pass
def split_cb(f, s):
    #print("File: {0}, Dimensione: {1}".format(f, s))
    pass


def merge(sourceFolder):
    fs.merge(input_dir=sourceFolder, callback=merge_cb)



def split(filePath, chunk, destinationFolder):
    global fs
    size = os.path.getsize(filePath) # Dimensione in bytes
    splitSize = math.ceil(size/chunk)
    fs.split(file=filePath, split_size=splitSize, output_dir=destinationFolder, callback=split_cb)

# -----------------------------------------

chooseMod()

if MOD == CLIENT_MODE:

    
    from clientPlot import clientPlot
    
    cp = clientPlot(FARM_IP, FARM_PORT)
    cp.socket_begin()
    cp.farm_connect()
    
    cp.sendChunk()
    
    cp.socket_close()


if MOD == SERVER_MODE:

    from serverPlot import serverPlot

    sp = serverPlot(FARM_IP, FARM_PORT, MAX_CONNECTIONS_ALLOWED)
    sp.socket_begin()
    sp.socket_bind()
    sp.socket_listen()
    sp.socket_close()
    
