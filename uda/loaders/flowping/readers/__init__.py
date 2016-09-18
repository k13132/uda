# -*- coding: utf-8 -*- 


class BaseFlowPingReader(object):
    """ 
    Abstract class defining uPing readers. Every properly behaved reader can read
    its data from given source logs. Right after that it caches this data into the
    current structure for later reading. Every next call only cached results are read
    """
    
    # uPing logs
    sl, cl= None, None
    gzip= False
    
    # uda.DataFrame
    df= None
    
    def __init__(self, server_log, client_log, gzip, *args, **kwargs ):
        # Default values
        self.ftype= kwargs.get('ftype','std')
        if 'ftype' in kwargs.keys(): del kwargs['ftype']        
        
        self.sl= server_log
        self.cl= client_log
        self.gzip= gzip
    
    def load(self):
        """ This function return uda.DataFrame of given data """
        raise NotImplementedError
    
    def getDataFrame(self):
        """ This function returns loaded data """
        
        # Load data if are not loaded
        if self.df is None:
            self.load()
        
        # Return data as uda.DataFrame
        return self.df



class BaseFlowPingReaderInterval(BaseFlowPingReader):
    
    window, _fwindow= None, 1.0
    how= 'median'
    def __init__(self, *args,  **kwargs ):
        
        # Default values
        self.window= kwargs.get('window',1)
        if 'window' in kwargs.keys(): del kwargs['window']

        self.how= kwargs.get('how','median')
        if 'how' in kwargs.keys(): del kwargs['how']
        
        # Original constructor
        super(BaseFlowPingReaderInterval, self ).__init__( *args, **kwargs)
        
        # Extra work with extra parameters
        _scales= {'s':1,'h':3600,'m':60,'d':24*3600, 'w':7*24*3600}
        # Convert int to datetime format
        if self.window.__class__.__name__ == 'int':
            self.window= '%ds'%self.window
            print self.window
            
        else:
            if not self.window[-1] in _scales.keys():
                self.window= '1s'
        
        self._fwindow= float( self.window[:-1]*_scales[ self.window[-1] ] )
