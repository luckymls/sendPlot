import logging
from typing import Callable, IO, Tuple, Optional
import csv
import time
import os
import ntpath


class Filesplit:
    
    def __init__(self) -> None:
        
        self.log = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self.man_filename = "fs_manifest.csv"
        self._buffer_size = 1000000  # 1 MB

    def __process_split(
        self,
        fi: IO,
        fo: IO,
        split_size: int,
        carrozzella: Optional[str],
        newline: bool = False,
        output_encoding: str = None,
        include_header: bool = False,
        header: str = None,
    ) -> Tuple:

        
        buffer_size = (
            split_size if split_size < self._buffer_size else self._buffer_size
        )
        buffer = 0
        if not newline:
            while True:
                if carrozzella:
                    fo.write(carrozzella)
                    buffer += (
                        len(carrozzella)
                        if not output_encoding
                        else len(carrozzella.encode(output_encoding))
                    )
                    carrozzella = None
                    continue
                chunk = fi.read(buffer_size)
                if not chunk:
                    break
                chunk_size = (
                    len(chunk)
                    if not output_encoding
                    else len(chunk.encode(output_encoding))
                )
                if buffer + chunk_size <= split_size:
                    fo.write(chunk)
                    buffer += chunk_size
                else:
                    carrozzella = chunk
                    break
            
            if not carrozzella: # Imposta su None se non c'è nessun dato disponibile
                carrozzella = None
            return carrozzella, buffer, None
        else:
            if carrozzella:
                if header:
                    fo.write(header)
                fo.write(carrozzella)
                if header:
                    buffer += (
                        len(carrozzella) + len(header)
                        if not output_encoding
                        else len(carrozzella.encode(output_encoding))
                        + len(header.encode(output_encoding))
                    )
                else:
                    buffer += (
                        len(carrozzella)
                        if not output_encoding
                        else len(carrozzella.encode(output_encoding))
                    )
                carrozzella = None
            for line in fi:
                if include_header and not header:
                    header = line
                line_size = (
                    len(line)
                    if not output_encoding
                    else len(line.encode(output_encoding))
                )
                if buffer + line_size <= split_size:
                    fo.write(line)
                    buffer += line_size
                else:
                    carrozzella = line
                    break
            
            if not carrozzella:
                carrozzella = None
            return carrozzella, buffer, header

    def split(
        self,
        file: str,
        split_size: int,
        output_dir: str = ".",
        callback: Callable = None,
        **kwargs,
    ) -> None:
        
        start_time = time.time()
        self.log.info("Inizio il processo di split file")

        newline = kwargs.get("newline", False)
        include_header = kwargs.get("include_header", False)
        
        if include_header:
            newline = True
        encoding = kwargs.get("encoding", None)
        split_file_encoding = kwargs.get("split_file_encoding", None)

        f = ntpath.split(file)[1]
        filename, ext = os.path.splitext(f)
        fi, man = None, None

        
        if split_file_encoding and not encoding:
            raise ValueError(
                "`encoding` deve essere specificato "
                "quando si passa `split_file_encoding`"
            )
        try:
            
            if encoding and not split_file_encoding:
                fi = open(file=file, mode="r", encoding=encoding)
            elif encoding and split_file_encoding:
                fi = open(file=file, mode="r", encoding=encoding)
            else:
                fi = open(file=file, mode="rb")
            # Crea il gestore file
            man_file = os.path.join(output_dir, self.man_filename)
            man = open(file=man_file, mode="w+", encoding="utf-8")
            # Crea il file manifest
            man_writer = csv.DictWriter(
                f=man, fieldnames=["filename", "filesize", "encoding", "header"]
            )
            
            man_writer.writeheader()

            split_counter, carrozzella, header = 1, "", None

            while carrozzella is not None:
                split_file = os.path.join(
                    output_dir, "{0}_{1}{2}".format(filename, split_counter, ext)
                )
                fo = None
                try:
                    if encoding and not split_file_encoding:
                        fo = open(file=split_file, mode="w+", encoding=encoding)
                    elif encoding and split_file_encoding:
                        fo = open(
                            file=split_file, mode="w+", encoding=split_file_encoding
                        )
                    else:
                        fo = open(file=split_file, mode="wb+")
                    carrozzella, output_size, header = self.__process_split(
                        fi=fi,
                        fo=fo,
                        split_size=split_size,
                        newline=newline,
                        output_encoding=split_file_encoding,
                        carrozzella=carrozzella,
                        include_header=include_header,
                        header=header,
                    )
                    if callback:
                        callback(split_file, output_size)
                    # Scrive il file manifest
                    di = {
                        "filename": ntpath.split(split_file)[1],
                        "filesize": output_size,
                        "encoding": encoding,
                        "header": True if header else None,
                    }
                    man_writer.writerow(di)

                    split_counter += 1
                finally:
                    if fo:
                        fo.close()
        finally:
            if fi:
                fi.close()
            if man:
                man.close()

        run_time = round((time.time() - start_time) / 60)

        self.log.info(f"Processo completato")
        self.log.info(f"Tempo impiegato(m): {run_time}")

    def merge(
        self,
        input_dir: str,
        output_file: str = None,
        manifest_file: str = None,
        callback: Callable = None,
        cleanup: bool = False,
    ) -> None:
        
        start_time = time.time()
        self.log.info("Inizio l'unione")

        if not os.path.isdir(input_dir):
            raise NotADirectoryError("La cartella non è valida")

        manifest_file = (
            os.path.join(input_dir, self.man_filename)
            if not manifest_file
            else manifest_file
        )
        if not os.path.exists(manifest_file):
            raise FileNotFoundError("Non trovo il manifest")

        fo = None
        clear_output_file = True
        header_set = False

        try:
            # Leggi i dati dal csv
            with open(file=manifest_file, mode="r", encoding="utf-8") as man_fh:
                man_reader = csv.DictReader(f=man_fh)
                for line in man_reader:
                    encoding = line.get("encoding", None)
                    header_avail = line.get("header", None)
                    
                    if not output_file:
                        f, ext = ntpath.splitext(line.get("filename"))
                        output_filename = "".join([f.rsplit("_", 1)[0], ext])
                        output_file = os.path.join(input_dir, output_filename)
                    # Inizio del merge
                    if clear_output_file:
                        if os.path.exists(output_file):
                            os.remove(output_file)
                        clear_output_file = False
                    
                    if not fo:
                        if encoding:
                            fo = open(file=output_file, mode="a", encoding=encoding)
                        else:
                            fo = open(file=output_file, mode="ab")
                    
                    try:
                        input_file = os.path.join(input_dir, line.get("filename"))
                        if encoding:
                            fi = open(file=input_file, mode="r", encoding=encoding)
                        else:
                            fi = open(file=input_file, mode="rb")
                        
                        if header_set:
                            next(fi)
                        for line in fi:
                            if header_avail and not header_set:
                                header_set = True
                            fo.write(line)
                    finally:
                        if fi:
                            fi.close()
        finally:
            if fo:
                fo.close()

        
        if cleanup:
        
            with open(file=manifest_file, mode="r", encoding="utf-8") as man_fh:
                man_reader = csv.DictReader(f=man_fh)
                for line in man_reader:
                    f = os.path.join(input_dir, line.get("filename"))
                    if os.path.exists(f):
                        os.remove(f)
            
            if os.path.exists(manifest_file):
                os.remove(manifest_file)

        
        if callback:
            callback(output_file, os.stat(output_file).st_size)

        run_time = round((time.time() - start_time) / 60)

        self.log.info(f"Processo completato")
        self.log.info(f"Tempo esecuzione(m): {run_time}")


