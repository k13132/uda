# -*- coding: utf-8 -*-

import uda
import numpy as np
from uda.loaders.flowping.readers import BaseFlowPingReader, BaseFlowPingReaderInterval
import pandas
import v10

class FlowPingReader(v10.FlowPingReader):

    """ 
    Basic uPing reader knowing only THE old uPing format where two logs 
    are necessary in order to do proper date assembly 
    """

    def _load_client(self):

        # Create right file pointer
        print 'client_log', self.cl,
        fp = open(self.cl, 'r')
        if self.gzip:
            import gzip
            print 'gzip',
            fp = gzip.open(self.cl, 'r')
        print '.'

        # Read log file line by line
        _c, i = [], 0
        _a = np.array([])
        for line in fp:
            _l = line.strip().split(';')[:-1]

            # Save heading
            if "C_TimeStamp" in line:
                _header = _l[:-1]
                continue
            

            try:
                if _l[1] == 'rx':
                    # From radek: 1407138077.666188;rx;64;localhost;100;0.10;2.005;255.36;     
                    t, direction_from, length, host, req, rtt, delta, rx = _l
                    tx = np.nan
                    direction_from = 'from'
                    
                else:
                    # To radek:  1407138077.666089;tx;64;;100;;2.004;;localhost;255.49;;;   
                    t, direction_from, length, s1, req, rtt, delta, host, rx, tx = _l[:-2]
                    rtt, rx = np.nan, np.nan
                    direction_from = 'to'                    
            except Exception, e:
                # Skip badly formated server logs
                print e
                continue
            
            _c.append([direction_from, t, length, req, rtt, delta, tx, rx])
            i += 1
            if i > 1e6:
                _a = np.append(_a, _c)
                _c = []

        _a = np.append(_a, _c)
        _a = _a.reshape(-1, 8)

        # Manage data in form of uda.DataFrame, basic data processing
        dfc = uda.DataFrame(_a)
        del _c, _a
        #dfc[0]= dfc[0].astype(bool)
        dfc[1] = dfc[1].astype(float)
        dfc[2], dfc[3] = dfc[2].astype(int), dfc[3].astype(int)
        dfc[4] = dfc[4].astype(float) / 1000
        dfc[5] = dfc[5].astype(float)
        dfc[6] = dfc[6].astype(float)
        dfc[7] = dfc[7].astype(float)
        dfc.index = dfc[3]

        # Data overhead
        labels = ['direction', 't', 'len', 'req', 'rtt', 'delta', 'tx', 'rx']
        cols = uda.MultiIndex.from_arrays([len(labels) * ['client'], labels])
        dfc.columns = cols

        # Return parsed data
        return dfc

    def _load_server(self):

        # Create right file pointer
        print 'server_log', self.sl,
        fp = open(self.sl, 'r')
        if self.gzip:
            import gzip
            print 'gzip',
            fp = gzip.open(self.sl, 'r')
        print '.'

        # Read log file line by line
        _c, i = [], 0
        _a = np.array([])
        for line in fp:
            _l = line.split(';')[:-1]
            
            # skip heading
            if "S_TimeStamp" in line: continue
            
            t, length, host, req, s1, delta, rx, tx = _l            
            _c.append([t, length, req, delta, rx, tx])
            i += 1
            if i > 1e6:
                _a = np.append(_a, _c)
                _c = []
        _a = np.append(_a, _c)
        _a = _a.reshape(-1, 6)

        # Manage data in form of uda.DataFrame, basic data processing
        df = uda.DataFrame(_a)
        del _c, _a
        df[0] = df[0].astype(float)
        df[1], df[2] = df[1].astype(int), df[2].astype(int)
        df[3] = df[3].astype(float) / 1000
        df[4] = df[4].astype(float)
        df[5] = df[5].astype(float)
        df.index = df[2]

        # Data overhead
        labels = ['t', 'len', 'req', 'delta', 'rx', 'tx', ]
        cols = uda.MultiIndex.from_arrays([len(labels) * ['server'], labels])
        df.columns = cols

        # Return parsed data
        return df

    def _merge(self, dfl, dfr):
        # Merge two given uda.DataFrames
        df = uda.merge(dfl, dfr, right_index=True, left_index=True)
        
        # loss indicator
        # Client
        for d in ['from', 'to']:
            fr = df['client']['direction'] == d
            _diff = df[fr]['client']['req'].diff() - 1
            df['client', 'loss_indicator_%s' % d] = _diff

        # Server
        _diff = df[fr]['server']['req'].diff() - 1
        df['server', 'loss_indicator'] = _diff

        # Return
        return df



class FlowPingReaderInterval(FlowPingReader, v10.FlowPingReaderInterval):

    pass
