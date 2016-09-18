# -*- coding: utf-8 -*- 

import uda
import numpy as np
from uda.loaders.flowping.readers import BaseFlowPingReader, BaseFlowPingReaderInterval


class FlowPingReader(BaseFlowPingReader):
    """ 
    Basic FlowPing reader needing 2 logs (client and server) to do proper data
    assembly - WITHOUT -E parameter.
    It can handle standard and CSV input (both logs must have same format).
    FlowPing ver.: 1.2.0 - 1.2.5
    """
    
    def _load_client_std(self, fp):
        '''Read standard log file line by line'''
        _c,i = [],0
        _a= np.array([])
        for line in fp:
            # [1400677563.055119] 250 bytes from 147.32.211.36: req=4 time=826.87 ms
            _l= line.split(' ')
            # new log file has first line empty and last has '/n' - why??? - TODO
            if len(_l) != 8:
                continue
            t,length,req, time = _l[0][1:-1], _l[1], _l[5].split('=')[1] , _l[6].split('=')[1]
            _c.append( [t,length,req, time] )
            i+=1
            if i > 1e6:
                _a= np.append(_a, _c )
                _c,i = [],0
        _a= np.append(_a, _c )
        _a=_a.reshape(-1,4)
        
        # Manage data in form of uda.DataFrame, basic data processing         
        dfc= uda.DataFrame(_a)
        del _c, _a
        
        # return dataframe
        return dfc
    
    def _load_client_csv(self, fp):
        '''Read CSV log file line by line'''
        _c,i = [],0
        fl = True   # first line
        _a= np.array([])
        for line in fp:
            # C_TimeStamp;C_Direction;C_PacketSize;C_From;C_Sequence;C_RTT;C_Delta;C_RX_Rate;C_To;C_TX_Rate;
            # 1403106334.906436;rx;500;147.32.211.36;1;0.61;;;
            # skip first line
            if fl == True:
                fl= False
                continue
            _l= line.split(';')
            t,length,req, time = _l[0], _l[2], _l[4], _l[5]
            _c.append( [t,length,req, time] )
            i+=1
            if i > 1e6:
                _a= np.append(_a, _c )
                _c,i = [],0
        _a= np.append(_a, _c )
        _a=_a.reshape(-1,4)
        
        # Manage data in form of uda.DataFrame, basic data processing         
        dfc= uda.DataFrame(_a)
        del _c, _a
        
        # return dataframe
        return dfc
    
    def _load_client(self):        
        # Create right file pointer
        print 'client_log', self.cl,
        fp= open(self.cl, 'r')
        if self.gzip:
            import gzip
            print 'gzip',
            fp= gzip.open(self.cl, 'r')
        print '.'
        
        # parse lines
        if self.ftype == 'std':
            dfc= self._load_client_std(fp)
        elif self.ftype == 'csv':
            dfc= self._load_client_csv(fp)
        
        dfc[0]= dfc[0].astype(float)
        dfc[1], dfc[2]= dfc[1].astype(int), dfc[2].astype(int)
        dfc[3]= dfc[3].astype(float)/1000
        dfc.index= dfc[2]
        
        # Data overhead
        labels1= ['client','server','client','client']
        labels2= ['recvts','size'  ,'req'   ,'rtt']
        cols= uda.MultiIndex.from_arrays([ labels1, labels2])
        dfc.columns= cols
        
        # Return parsed data
        return dfc
    
    def _load_server_std(self, fp):
        '''Read standard log file line by line'''
        _c,i = [],0
        _a= np.array([])
        for line in fp:
            # [1400677562.786596] 250 bytes from 37.48.45.70: req=3 ttl=xx delta=161.183 ms
            _l= line.split(' ')
            if len(_l) != 9:
                continue
            t,length,req, delta = _l[0][1:-1], _l[1], _l[5].split('=')[1] , _l[7].split('=')[1]
            _c.append( [t,length,req, delta] )
            i+=1
            if i > 1e6:
                _a= np.append(_a, _c )
                _c,i = [],0                
        _a= np.append(_a, _c )
        _a=_a.reshape(-1,4)
        
        # Manage data in form of uda.DataFrame, basic data processing
        df= uda.DataFrame(_a)
        del _c, _a
        
        # return dataframe
        return df
    
    def _load_server_csv(self, fp):
        '''Read CSV log file line by line'''
        _c,i = [],0
        fl = True   # first line
        _a= np.array([])
        for line in fp:
            # S_TimeStamp;S_PacketSize;S_From;S_Sequence;S_TTL;S_Delta;S_RX_Rate;S_TX_Rate;
            # 1403106334.905881;500;147.32.211.109;1;xx;199.951;;;
            # skip first line
            if fl == True:
                fl= False
                continue
            _l= line.split(';')
            t,length,req, delta = _l[0], _l[1], _l[3], _l[5]
            _c.append( [t,length,req, delta] )
            i+=1
            if i > 1e6:
                _a= np.append(_a, _c )
                _c,i = [],0
        _a= np.append(_a, _c )
        _a=_a.reshape(-1,4)
        
        # Manage data in form of uda.DataFrame, basic data processing
        df= uda.DataFrame(_a)
        del _c, _a
        
        # return dataframe
        return df    
    
    def _load_server(self):
        
        # Create right file pointer
        print 'server_log', self.sl,
        fp= open(self.sl, 'r')
        if self.gzip:
            import gzip
            print 'gzip',
            fp= gzip.open(self.sl, 'r')
        print '.'
        
        # parse lines
        if self.ftype == 'std':
            df= self._load_server_std(fp)
        elif self.ftype == 'csv':
            df= self._load_server_csv(fp)
        
        df[0]= df[0].astype(float)
        df[1], df[2]= df[1].astype(int), df[2].astype(int)
        df[3]= df[3].astype(float)/1000
        df.index= df[2]
        
        labels1= ['server','client','server','server']
        labels2= ['recvts','size'  ,'req'   ,'delta' ]
        cols= uda.MultiIndex.from_arrays([ labels1, labels2])
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
        df['client','sendts']= df['client']['recvts']-df['client']['rtt']
        df['global','c2s_delay']= df['server']['recvts']-df['client']['sendts']
        df['global','s2c_delay']= df['client']['recvts']-df['server']['recvts']
        df['client','loss_indicator']= df['client']['req'].diff()-1
        df['server','loss_indicator']= df['server']['req'].diff()-1
              
        # Set the index to be based on time of client, however shifted to 0 
        print df
        df.index= uda.to_datetime( df['client']['sendts'] - df.iloc[0]['client']['sendts'] ,unit='s')
#        df.index= df['client']['sendts'] - df.ix[1]['client']['sendts']

        # http://stackoverflow.com/questions/19026684/convert-float-series-into-an-integer-series-in-pandas
        # However it makes it slow
        df['client','sendts']= uda.to_datetime( df['client','sendts'],unit='s')
        df['client','recvts']= uda.to_datetime( df['client','recvts'],unit='s')
        df['server','recvts']= uda.to_datetime( df['server','recvts'],unit='s')
                
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
