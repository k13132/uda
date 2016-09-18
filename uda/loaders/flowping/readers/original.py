# -*- coding: utf-8 -*- 

import uda
import numpy as np
from uda.loaders.flowping.readers import BaseFlowPingReader, BaseFlowPingReaderInterval


class FlowPingReader(BaseFlowPingReader):
    """ 
    Basic uPing reader knowing only THE old uPing format where two logs 
    are necessary in order to do proper date assembly 
    """
    def _load_client(self):
        
        # Create right file pointer
        print 'client_log', self.cl,
        fp= open(self.cl, 'r')
        if self.gzip:
            import gzip
            print 'gzip',
            fp= gzip.open(self.cl, 'r')
        print '.'
        
        # Read log file line by line
        _c,i = [],0
        _a= np.array([])
        for line in fp:
            _l= line.split(' ')
            t,length,req, time = _l[0][1:-1], _l[1], _l[5].split('=')[1] , _l[6].split('=')[1]
            _c.append( [t,length,req, time] )
            i+=1
            if i > 1e6:
                _a= np.append(_a, _c )
                _c= []
        _a= np.append(_a, _c )
        _a=_a.reshape(-1,4)
        
        # Manage data in form of uda.DataFrame, basic data processing         
        dfc= uda.DataFrame(_a)
        del _c, _a
        dfc[0]= dfc[0].astype(float)
        dfc[1], dfc[2]= dfc[1].astype(int), dfc[2].astype(int)
        dfc[3]= dfc[3].astype(float)/1000
        dfc.index= dfc[2]
        
        # Data overhead
        labels= ['t','len','req','time']
        cols= uda.MultiIndex.from_arrays([ len(labels)*['client'], labels])
        dfc.columns= cols
        
        # Return parsed data
        return dfc
        
    
    def _load_server(self):
        
        # Create right file pointer
        print 'server_log', self.sl,
        fp= open(self.sl, 'r')
        if self.gzip:
            import gzip
            print 'gzip',
            fp= gzip.open(self.sl, 'r')
        print '.'
        
        # Read log file line by line
        _c,i = [],0
        _a= np.array([])
        for line in fp:
            _l= line.split(' ')
            t,length,req, delta = _l[0][1:-1], _l[1], _l[5].split('=')[1] , _l[7].split('=')[1]
            _c.append( [t,length,req, delta] )
            i+=1
            if i > 1e6:
                _a= np.append(_a, _c )
                _c= []
        _a= np.append(_a, _c )
        _a=_a.reshape(-1,4)
        
        # Manage data in form of uda.DataFrame, basic data processing
        df= uda.DataFrame(_a)
        del _c, _a
        df[0]= df[0].astype(float)
        df[1], df[2]= df[1].astype(int), df[2].astype(int)
        df[3]= df[3].astype(float)/1000
        df.index= df[2]
        
        labels= ['t','len','req','delta']
        cols= uda.MultiIndex.from_arrays([ len(labels)*['server'], labels])
        df.columns= cols

        # Return parsed data
        return df
        
        
    def _merge(self, dfl, dfr):
        # Merge two given uda.DataFrames
        df= uda.merge(dfl,dfr, right_index=True, left_index=True)
        
        # Return 
        return df
        
    
    def load(self):
        """ Public method loading data from given logs """
        # Obtain data
        _dfc= self._load_client()
        _dfs= self._load_server()
        
        # Merging 
        df= uda.DataFrame( self._merge(_dfc, _dfs) )
        
        # Calculate extra columns
        df['global','rtt']= df['client']['t']-df['server']['t']
        df['client','loss_indicator']= df['client']['req'].diff()-1
        df['server','loss_indicator']= df['server']['req'].diff()-1
              
        # Set the index to be based on time of client, however shifter to 0 
        df.index= uda.to_datetime( df['client']['t'] - df.ix[1]['client']['t'] ,unit='s')

        # http://stackoverflow.com/questions/19026684/convert-float-series-into-an-integer-series-in-pandas
        # However it makes it slow
        df['client','t']= uda.to_datetime( df['client','t'],unit='s')
        df['server','t']= uda.to_datetime( df['server','t'],unit='s')
                
        # Set to by available by system
        self.df= df


class FlowPingReaderInterval(FlowPingReader, BaseFlowPingReaderInterval):
    def load(self):
        # Load data into df
        super(FlowPingReaderInterval, self ).load()
        
        # Resample data in df
        df= self.df.resample(self.window, how=self.how)
              
        # Packet loss
        df['client','packet_loss']= self.df['client']['loss_indicator'].resample(self.window, how='sum')/self._fwindow
        df['server','packet_loss']= self.df['server']['loss_indicator'].resample(self.window, how='sum')/self._fwindow
        
        # Drop of old columns
        del df['server']['loss_indicator']
        del df['client']['loss_indicator']
        
        # Final save
        self.df= df
