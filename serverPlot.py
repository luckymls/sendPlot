import socket, tqdm, os


class serverPlot:


    SEPARATOR = "<SEPARATOR>"
    BUFFER_SIZE = 4096 # KB
     
    config_farm = {
        'host': None,
        'port': None,
        "max_connections": None
        }

    def __init__(self, host, port=5001, max_connections=6):

        self.config_farm['host'] = host
        self.config_farm['port'] = port
        self.max_connections['max_connections'] = max_connections

    def log(self, message="\n"):
        print(message)

    def socket_begin(self):
        self.s = socket.socket()
        self.log("Attivo listener...")

    def socket_bind(self):
        self.log("Configuro parametri connessione.")
        self.s.bind((self.config_farm['host'], self.config_farm['port']))
        self.log("Parametri connessione configurati")

    def socket_listen(self):
        print("Listener attivato.")
        self.s.listen(self.config_farm['max_connections'])
        client_socket,address = self.client_accept()
        filename, filesize = self.on_file_received(client_socket)
        self.download_chunk(client_socket, filename, filesize)
        

    def socket_close(self):
        self.s.close()

    def client_close(self, client_socket):
        client_socket.close()
        
    def client_accept(self):
        client_socket, address = self.s.accept()
        self.log("Connessione di %s accettata" % address)    
        return [client_socket, address]

    def on_file_received(self, client_socket):
        r = client_socket.recv(self.BUFFER_SIZE).decode()
        self.log(received)
        filename, filesize = r.split(self.SEPARATOR)
        filename = os.path.basename(filename)
        filesize = int(filesize)
        return [filename, filesize]
    
    def download_chunk(self, client_socket, filename, filesize):
        progress = tqdm.tqdm(range(filesize), "Ricevo %s" % filename, unit="B", unit_scale=True, unit_divisor=1024)
        with open(filename, "wb") as f:
            while True:
            
                bytes_read = client_socket.recv(BUFFER_SIZE) # Leggo
                if not bytes_read: # Fine 
                    break
                f.write(bytes_read)
                progress.update(len(bytes_read))
