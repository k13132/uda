# -*- coding: utf-8 -*- 

import os
from readers import original as cur
#from readers import v10 as cur


class FlowPing(object):
    """ General FlowPing loader """
        
    # Measurement description
    name= None
    server_log= None
    client_log= None
    gzip= False
    
    # uPingReader
    reader= None
    
    #readers.original.uPingReader
    def __init__(self, name=None, server_log=None, client_log=None, gzip=False, reader=cur.FlowPingReader, **kwargs ):
        self.server_log= server_log
        self.client_log= client_log
        self.gzip= gzip
        
        # Reader
        self.reader= reader(server_log, client_log, gzip, **kwargs)
    
    def getDataFrame(self):
        return self.reader.getDataFrame()    
