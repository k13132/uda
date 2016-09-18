import uda
from uda.loaders.omnetpp.readers.base import *
import numpy

CACHE_INTERVAL= 10000

class VciReader(ConfigReaderMixin, VectorNameReaderMixin, BaseReader):
    """ 
        VciReader is a simple class allowing to read statistical information of simulations
        - It requires path to the .vci file
    """
    
    vectors= None
    vectors_names= None
    parameters= None
    vci = None
    
    def __init__(self, vci):
        
        self.vci= vci
        self.fp= open(vci, 'r')        
        
        # Read the config file
        self._read_config()        
        self.parameters= self.config['iterationvars']
        
        # Load the data and create data fro Omnetpp object
        self.vectors= []   
        self.vectors_names = []
        self._load_vector_names()
    
    
    def _load_data(self):        
        
        cache= []   
        a= numpy.array([])
        for line in self.fp:
            _stripes= line.strip().split('\t')
            if _stripes.__len__() != 1:                
                
                # Vector data.
                vid, data= _stripes
                vid= int(vid)
                
                # Skip not interesting vectors
                if not vid in self.vectors: continue
            
                data= data.split(' ')
                data.append(vid)
                
                cache.append( data )
                if cache.__len__() >= CACHE_INTERVAL:
                    a= numpy.append( a, cache )
                    cache= []
                    
        if cache.__len__() > 0:
            a= numpy.append( a, cache )
            del cache
        
        _heading= ['offset', 'length','firstEventNo','lastEventNo','firstSimtime','lastSimtime','count','min','max','sum','sqrsum', 'vid']
        df= uda.DataFrame(a.reshape(-1,len(_heading)), columns=_heading).astype(float)
        df['mean']= df['sum']/df['count']    
        df['vid'] = df['vid'].astype(int)
        df= df.set_index('vid')    
        self.df= df

