from fileSplit import Filesplit
import os,math
import ftplib


FTP_HOST = ""
FTP_USER = ""
FTP_PASS = ""
ftp=None

sizeSent = 0
fileTotalSize = 0


# -------- SEND FILE --------

# Future
def progressSendPlot(block):
    global sizeSent

    sizeSent+=1024
    percentComplete = (sizeSent/fileTotalSize)*100

    print("Percentuale: "+percentComplete+"%", flush=True)
    

# Future
def sendPlot(plot):
    global sizeSent,fileTotalSize
    
    sizeSent=0
    filePath = plot
    fileTotalSize = os.path.getsize(plot)
    with open(plot, "rb") as file:

        ftp.storbinary(f"STOR {filename}", file, callback=progressSendPlot)


# Future
def handleSendPlot(folder):
    global ftp
    
    ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
    ftp.encoding = "utf-8"

    listFile = os.scandir(folder)

    for file in listFile:
        
        if file.is_file():

            if file.name.endswith('csv'):
                    
                print(folder+file.name)


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

global fs
fs = Filesplit()


split(input("Inserire nome file: "), 1000, "end/")

input("Premi un tasto qualsiasi per effettuare il merge dei file")

merge("end/")
