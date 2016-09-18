# -*- coding: utf-8 -*- 

import uda
import numpy as np
from uda.loaders.flowping.readers import BaseFlowPingReader, BaseFlowPingReaderInterval


class FlowPingReader(BaseFlowPingReader):
    """ 
    Basic FlowPing reader needing at least 1 log (client and optionally server)
    to do proper data assembly - WITH -E parameter.
    It can handle standard and CSV input (both logs must have same format).
    FlowPing ver.: 1.2.0 - 1.2.5 
    """
    # TODO opravit a zkontrolovat
    def _load_client_std(self, fp):
        '''Read standard log file line by line'''
        _c,i = [],0
        _a= np.array([])
        for line in fp:
            _l= line.split(' ')
            try:
                if _l[3] == 'from': 
                    # From line: [1407249150.638280] 500 bytes from 147.32.211.36: req=10 time=2.34 ms
                    t, length, req, time = _l[0][1:-1], _l[1], _l[5].split('=')[1] , _l[6].split('=')[1]
                    delta, rx = np.nan, np.nan  
                    tx= np.nan
                    direction= 'from'
                elif _l[3] == 'to':
                    # To line:  [1407249150.635936] 500 bytes to 147.32.211.36: req=10 delta=10.835 ms tx_rate=369.17 kbit/s
                    t,length,req, delta, tx = _l[0][1:-1], _l[1], _l[5].split('=')[1] , _l[6].split('=')[1], _l[8].split('=')[1]
                    time, rx= np.nan, np.nan
                    direction= 'to'
            except Exception, e:
                # Skip badly formated server logs
                continue

            _c.append( [direction, t, length, req, time, delta, tx, rx] )
            i+=1
            if i > 1e6:
                _a = np.append(_a, _c)
                _c, i = [], 0
        
        _a= np.append(_a, _c)
        _a=_a.reshape(-1,8)
        
        # Manage data in form of uda.DataFrame, basic data processing         
        dfc= uda.DataFrame(_a)
        del _c, _a
        
        # Return parsed data
        return dfc
         
    def _load_client_csv(self, fp):
        '''Read CSV log file line by line'''
        _c, i = [], 0
        fl = True   # first line
        _a= np.array([])
        for line in fp:
            # C_TimeStamp;C_Direction;C_PacketSize;C_From;C_Sequence;C_RTT;C_Delta;C_RX_Rate;C_To;C_TX_Rate;
            # 1407249320.544665;rx;500;147.32.211.36;1;0.99;;;
            # 1407249320.543673;tx;500;;1;;10.840;;147.32.211.36;369.00;;;
            # skip first line
            if fl == True:
                fl= False
                continue
            _l= line.split(';')
            try:
                if  _l[1] == 'rx': 
                    # From line: 1407249320.544665;rx;500;147.32.211.36;1;0.99;;;
                    t, length, req, time = _l[0], _l[2], _l[4], _l[5]
                    delta, rx = np.nan, np.nan
                    tx= np.nan
                    direction_from= 'from'
                elif _l[1] == 'tx':
                    # To line:  1407249320.543673;tx;500;;1;;10.840;;147.32.211.36;369.00;;;
                    t, length, req, delta, tx = _l[0], _l[2], _l[4], _l[6], _l[9]
                    time, rx = np.nan, np.nan
                    direction_from= 'to'
            except Exception, e:
                # Skip badly formated server logs
                continue

            _c.append( [direction_from, t,length,req, time, delta,tx, rx] )
            i+=1
            if i > 1e6:
                _a= np.append(_a, _c)
                _c, i = [], 0
        
        _a= np.append(_a, _c )
        _a=_a.reshape(-1,8)
        
        # Manage data in form of uda.DataFrame, basic data processing         
        dfc= uda.DataFrame(_a)
        del _c, _a

        # Return parsed data
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
        
        #dfc[0]= dfc[0].astype(bool)
        dfc[1]= dfc[1].astype(float)
        dfc[2], dfc[3]= dfc[2].astype(int), dfc[3].astype(int)
        dfc[4]= dfc[4].astype(float)/1000
        dfc[5]= dfc[5].astype(float)
        dfc[6]= dfc[6].astype(float)
        dfc[7]= dfc[7].astype(float)
        dfc.index= dfc[3]
        
        # Data overhead
        labels= ['direction','t','len','req','time', 'delta', 'tx','rx']
        cols= uda.MultiIndex.from_arrays([ len(labels)*['client'], labels])
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
            t, length, req, delta = _l[0][1:-1], _l[1], _l[5].split('=')[1] , _l[7].split('=')[1]
            rx, rx_unit, tx, tx_unit = np.nan, np.nan, np.nan, np.nan
            _c.append( [t,length,req, delta, rx, tx] )
            i+=1
            if i > 1e6:
                _a= np.append(_a, _c )
                _c,i = [],0                
        _a= np.append(_a, _c )
        _a=_a.reshape(-1,6)
        
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
            rx, rx_unit, tx, tx_unit = np.nan, np.nan, np.nan, np.nan
            _c.append( [t,length,req, delta, rx, tx] )
            i+=1
            if i > 1e6:
                _a= np.append(_a, _c )
                _c,i = [],0
        _a= np.append(_a, _c )
        _a=_a.reshape(-1,6)
        
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
        df[4]= df[4].astype(float)
        df[5]= df[5].astype(float)
        df.index= df[2]
        
        # Data overhead
        labels= ['t','len','req','delta', 'rx', 'tx', ]
        cols= uda.MultiIndex.from_arrays([ len(labels)*['server'], labels])
        df.columns= cols

        # Return parsed data
        return df
        
        
    def _merge(self, dfl, dfr):
        # Merge two given uda.DataFrames
        df= uda.merge(dfl,dfr, right_index=True, left_index=True)
        df['global','rtt']= (df['server']['t']-df[ df['client']['direction']=='to'  ]['client']['t']).dropna()
        
        ## loss indicator
        # Client
        for d in ['from','to']:
            fr=df['client']['direction']==d
            _diff= df[fr]['client']['req'].diff()-1
            df['client','loss_indicator_%s'%d]= _diff
        
        # Server
        _diff= df[fr]['server']['req'].diff()-1
        df['server','loss_indicator']= _diff
        
        
        # Return 
        return df
        
    
    def load(self):
        """ Public method loading data from given logs """
        # Obtain data
        _dfc= self._load_client()
        _dfs= self._load_server()
        
        # Merging 
        df= uda.DataFrame( self._merge(_dfc, _dfs) )
        
        # Retiming (time must start from 0)
        for item in ['client', 'server']:
            df[item]['t']     = df[item]['t'] - min(df[item]['t'])
            # Slow eparatons
            df[item]['t']     = uda.to_datetime( df[item]['t'],unit='s')
            
        # Set the index to be based on time of client, however shifter to 0 
        df.index= df['client']['t']-min(df['client']['t'])
        df.index= uda.to_datetime( df.index, unit='s' )
        
        # Set to by available by system
        self.df= df



class FlowPingReaderInterval(FlowPingReader, BaseFlowPingReaderInterval):
   
    def load(self):
        # Load data into df
        super(FlowPingReaderInterval, self ).load()
        
        # Resample data in df
        # Server
        _server= self.df['server'].resample(self.window, how=self.how)
        _global= self.df['global'].resample(self.window, how=self.how)
        del _server['loss_indicator']
        
        # Client TO
        _to= self.df[ self.df['client']['direction']=='to' ]
        del _to['client','direction']
        _to = _to['client'].resample(self.window, how=self.how)
        del _to['loss_indicator_from']
        del _to['loss_indicator_to']
        _to['direction']= 'to'
        
        # Client FROM
        _from= self.df[ self.df['client']['direction']=='from' ]
        del _from['client','direction']
        _from = _from['client'].resample(self.window, how=self.how)
        del _from['loss_indicator_from']
        del _from['loss_indicator_to']
        _from['direction']= 'from'
        
        # Packet loss
        _from['packet_loss_from']= self.df['client']['loss_indicator_from'].resample(self.window, how='sum')/self._fwindow
        _to['packet_loss_to']= self.df['client']['loss_indicator_to'].resample(self.window, how='sum')/self._fwindow
        _server['packet_loss']= self.df['server']['loss_indicator'].resample(self.window, how='sum')/self._fwindow
        
        # Fix data titles
        _from.columns= uda.MultiIndex.from_arrays( [len( _from.columns.values )*['client',], _from.columns.values] )  
        _to.columns= uda.MultiIndex.from_arrays( [ len( _to.columns.values )*['client',], _to.columns.values ])
        _server.columns= uda.MultiIndex.from_arrays( [len( _server.columns.values )*['server',], _server.columns.values] )
        _global.columns= uda.MultiIndex.from_arrays( [len( _global.columns.values )*['global',], _global.columns.values] )
        
        # Merge to to DF
        _client= uda.concat( [_from, _to ])
        _gl_cl = uda.merge(_global, _client, right_index=True, left_index=True)
        self.df= uda.merge(_server, _gl_cl, right_index=True, left_index=True)
