# -*- coding: utf-8 -*-
"""
Created on Thu Mar 06 13:20:53 2014

@author: milos
"""

import numpy as np
from uda.loaders.omnetpp.readers.vecReader.base_interval import VecReader as BaseIntervalVecReader
from uda.loaders.omnetpp.readers.base import BaseReader
from uda.loaders.omnetpp import Omnetpp, VectorFilter
from uda.extra import hypo_linregres
import gzip
import sys
import uda
import os

SIMNAME= sys.argv[1]
RUNID= int(sys.argv[2])



class ScanVec(BaseReader):
    hist_max= 0
    def _load_data(self):
        print 'Urcuji parametetry'
        
        self._fp_vec= open(self.vec, 'r')
        self._fp_vec.seek(self._data_start_tell)
        
        # Walk through the file
        for line in self._fp_vec:
            
            _data= line.split('\t')
            try:
              _cvid= int( _data[0] )
            except:
                continue
            
            # Skip not interesting vectors
            if not _cvid in self.vectors: continue
            if not _cvid in HIST_VECTORS: continue    
                
            # Parse data
            try:
                t= float(_data[2])
            except:
                print line
                
            _m= int(_data[3])
            if _m > self.hist_max:
                self.hist_max= _m    
            
            
        self._fp_vec.close()
        super( ScanVec, self )._load_data()
    

class MyVecReader(ScanVec, BaseIntervalVecReader):
   
    def col_mean(self,vid=None,x=None,y=None, heading=False):
        if heading: return ('mean',)
        return np.mean(y)

    def col_median(self,vid=None,x=None,y=None, heading=False):
        if heading: return ('median',)
        return np.median(y)  
    
    def col_length(self,vid=None,x=None,y=None, heading=False):
        if heading: return ('n',)
        return x.__len__()
    

    def col_hist(self,vid=None,x=None,y=None, heading=False):
        if heading: return tuple( ['hist_bypass']+['hist_%s'%x for x in range(1,self.hist_max+1) ] )
        if not vid in HIST_VECTORS: return tuple( [np.nan]*(self.hist_max+1) )
        
        _hist= np.zeros(self.hist_max+1)
        for i in y:            
            _hist[ int(i) ]+= 1            
                
        return tuple(_hist)

def get_vector_tuple(omn, vid):
    vects= omn.get_vectors_names()
    for v in vects:
        if v[0] == vid:
            return (v[2],v[1])  # Prvni je parametr a druhy je zdroj
            return (v[1],v[2])  # Prvni je zdroj a druhy je parametr
    return ('error','error')


def apply_hypo_linregres(S):    
    return hypo_linregres( list(S.index), list(S.values) )    
    
def getDataFrameProperties(df):
    # Zakladni stat parametry vstupniho souboru df
    describe= uda.DataFrame( df.describe() )
    describe.columns= [0, ]
    
    # Testovani lineariy
    H= [ 'lin_b1', 'lin_b0', 'lin_r_value', 'lin_p_value', 'lin_stdErr', 'lin_crit' ]    
    dfH= apply_hypo_linregres(df)
    df_hypo= uda.DataFrame( list(dfH), index=H )
    
    # Urceni medianu
    df_median= uda.DataFrame( [df.median()], index=['median'] )
    
    # t_min a t_max merene veliciny
    t_mm= uda.DataFrame( [min(df.index), max(df.index) ], index=['t_min','t_max'] )
             
    # Spojeni dat v celek
    df_new= uda.concat( [df_median, describe, df_hypo, t_mm] )
    df_new.reset_index(inplace=True)
    
    # ;-)
    return df_new
  
def getDataFramePropertiesPerPartes(df, col):
    """ Metoda, ktera umoznuje sekvencni analyzu. Pri vyuziti unstack (predchozi pristup)
    jsem transformaci matice dosel k velmi ridke matici, coz vedlo k extremnimu vyuziti pameti.. """
    
    l= []
    for vid in df.index.levels[0]:
        pdf= getDataFrameProperties( df.xs(vid)[col] )
        pdf['vid'] = vid
        pdf.columns=['metric','value','vid']
        l.append( pdf )
    
    df_new= uda.concat( l )
    df_new= df_new.set_index(['vid','metric'])
    df_new= df_new.unstack(level=0)
    df_new.columns= [ int(x[1]) for x in df_new.columns ]
    df_new.reset_index(inplace=True)
    
    return df_new

def getOeHistDataframe(df):
    
    l = []
    for vid in HIST_VECTORS:        
        _df= df.xs(vid)
        _hist= filter( lambda x: 'hist_' in x, _df.columns )
        _df1= uda.DataFrame( _df[ _hist ].sum() )        
        _df2= uda.DataFrame( [ len(_hist)-1] , index=['max'])
        pdf= uda.concat( [_df1, _df2] )
        pdf['vid'] = vid
        pdf.reset_index(inplace=True)        
        pdf.columns=['metric','value','vid']
        l.append( pdf )
    
    # Format the table from lines to rows
    df_new= uda.concat( l )
    df_new= df_new.set_index(['vid','metric'])
    df_new= df_new.unstack(level=0)
    df_new.columns= [ int(x[1]) for x in df_new.columns ]
    df_new.reset_index(inplace=True)

    # Return final table
    return df_new    
    
def compress_file(fp):
    f_in = open(fp, 'rb')
    f_out = gzip.open('%s.gz'%fp, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
    os.unlink(fp)
    

## Nacteni dat z simulacnich vysledku
o= Omnetpp(SIMNAME, RUNID,vecreader=MyVecReader)

# O/E couting filter
foe= VectorFilter()
foe.addFilter(var="Number of O/E blocks used [-]") 
HIST_VECTORS= foe.filter(o)

# Setrici filter
fo= VectorFilter()
fo.addFilter(  var="Burst buffering probability")
fo.addFilterOR(var="Secondary contention ratio")
fo.addFilterOR(var="Buffering delay [s]")
fo.addFilterOR(var="The number of buffering (along the path)")
fo.addFilterOR(var="The number of bypass (along the path)")
fo.addFilterOR(var="End-to-End Delay from")
fo.addFilterOR(var="Bypass inclination")
fo.addFilterOR(var="Buffering inclination")
fo.addFilterOR(var="Throughput from")
fo.addFilterOR(var="Number of O/E blocks used [-]")
fo.addFilterOR(var="Access delay [s]")

v= fo.filter(o)
o.set_vectors( v )

# Urceni statistickych vlastnosti vektoru
print '"****** Loading data'
df= o.vec.getDataFrame()

print '"****** Cache data'
fp_df="%s.cache"%o._vec
df.to_pickle( fp_df )
compress_file( fp_df )

print '"****** Analyzing data'
data_median= getDataFramePropertiesPerPartes( df, 'median' )
data_mean= getDataFramePropertiesPerPartes( df, 'mean' )
data_oehist= getOeHistDataframe(df)

# Doplneni parametru simulace k datum
df_mean  = uda.DataFrame( dict( o.config['iterationvars2'].items() + [('method','mean')] + data_mean.to_dict().items() )  )
df_median= uda.DataFrame( dict( o.config['iterationvars2'].items() + [('method','median')] + data_median.to_dict().items() )  )
df_oehist= uda.DataFrame( dict( o.config['iterationvars2'].items() + [('method','hist')] + data_oehist.to_dict().items() )  )
df_data= uda.concat( [df_mean,df_median, df_oehist] )

# Extra information
df_data['runid']= o.config['runnumber']
df_data['seedset']= o.config['seedset']
df_data['t_max']= max( df.index.levels[1] )

# Opraveni zahlavi na lidsky citelne z predchozich celocistelnych identifikatoru vektoru
human=  [ get_vector_tuple(o,x) for x in df_data.columns if not x.__class__.__name__ == 'str' ]
params= [ ('parameters',x) for x in df_data.columns if x.__class__.__name__ == 'str' ]
df_data.reset_index(inplace=True)
del df_data['index']
df_data.columns = uda.MultiIndex.from_tuples( human+params )

# Verify whether the simulation finished OK
d= ( df_data['parameters','$duration'] - df_data['parameters','t_max'] )/df_data['parameters','$duration']
OKKO= d.median() < 0.1 and '.' or '.ko.'

# Export results
print "****** Results (%s/%s%scache) ******"%(SIMNAME,RUNID,OKKO)
df_data.to_csv(sys.stdout)
os.system('mkdir %s -p'%SIMNAME)
df_data.to_pickle("%s/%s%scache"%(SIMNAME,RUNID,OKKO))

print "****** Remove .vec file"
o.vec.fp.close()
os.unlink( o.vec.vec )