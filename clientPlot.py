# Guida per l'installazione:
# Windows: py -m pip install tqdm
# Unix: pip install tqdm

import os, socket, tqdm


class clientPlot:

    SEPARATOR = "<SEPARATOR>"
    BUFFER_SIZE = 4096 # KB
    config = {
        "host": None,
        "port": None,
    }
    
    def __init__(self, host, port=5001):
        self.config.host = host
        self.config.port = port

    def log(self, message="\n"):
        print(message)

    def socket_begin(self):
        self.s = socket.socket()
        self.log("Connetto alla FARM %s:%s" % (self.config['host'], self.config['port']))

    def socket_close(self):
        self.s.close()
        log("Connessione chiusa.")

    def farm_connect(self):
        try:
            self.s.connect((self.config['host'], self.config['port']))
        except:
            self.log("Connessione alla FARM non riuscita")
        else:
            self.log("Client connesso alla FARM")
    

    def sendChunk(self, chunk):
        chunkSize = os.path.getsize(chunk)
        self.s.send(("%s%s%s"%(chunk, self.SEPARATOR, chunkSize)).encode())
        progress = tqdm.tqdm(range(chunkSize), "Invio %s" % chunk, unit="B", unit_scale=True, unit_divisor=1024)
        with open(chunk, "rb") as f:
            while True:
                
                bytes_read = f.read(BUFFER_SIZE)
                if not bytes_read: # Fine trasmissione plot      
                    break
                
                self.s.sendall(bytes_read)   
                progress.update(len(bytes_read))






    
    
