# -*- coding: utf-8 -*- 

import os
import uda
from uda.loaders.omnetpp.readers.base import *
import numpy

CACHE_INTERVAL=100000

class VecReader(ConfigReaderMixin, VectorNameReaderMixin, BaseReader):
    """ Class that only reads .vec files of OMNeT++. 
        Returns DataFrame of data, empty spaces are replaced by NA
        """
        
    # Input .vec file
    vec, vcache=  None, None

    # Vector of vector statistics
    _vectors= set()

    # Statistically signicifant time interval <Tmin;Tmax>
    Tmin,Tmax = None, None 
    
    # To process
    vectors = None
    vectors_names= None
    
    def __init__(self, vec, tmin=0, tmax=float("inf"), vectors=None):
        """ Initialization of .vec readeru """
        
        # Access given .vec file
        self.vec = vec
        self.fp= open(vec, 'r')        
        self.vcache= "%s.cache"%vec
        
        # Time interval
        self.Tmin = tmin
        self.Tmax = tmax
        
        # Initialize set of required vectors
        self.vectors= vectors
        
        # Load simulation configuration
        self._read_config()
        self.parameters= self.config['iterationvars']


    def _load_cache(self):
        if os.path.exists( self.vcache ):
            print 'Loading from cache'
            self.df= uda.read_pickle(self.vcache)
            
    
    def _load_data(self):
        ### Read cached data     
        if self._load_cache(): return
        
        ### Load data
        _cache, i, data = [], 0, numpy.array([])        
        with open(self.vec, "r") as fp:
            for line in fp:
                _data= line.split('\t')
                try:
                    # Read the Vector ID
                    vid= int(_data[0])
                    
                    # Process only elected vectors
                    if not vid in self.vectors: continue
                    
                    # Read the time mark
                    t= float(_data[2])
                    
                    # T min allignment
                    if t<self.Tmin: continue
                    
                    # T max allignment                    
                    if t>self.Tmax and vid in self._vectors:
                        if len(self._vectors) == 0: break
                        
                    if vid not in self._vectors:
                        self._vectors.add(vid)
                    
                    # Adding lines to cache 
                    _line= [ float(x) for x in _data ]                    
                    _cache.append( _line )
                    i+=1
                    if i > CACHE_INTERVAL:
                        i=0
                        data= numpy.append( data, _cache )
                        _cache = []
                        
                    
                except Exception,e:
                    pass
                
        
        # Translate to Pandas DataFrame
        data= numpy.append( data, _cache )
        self.df= uda.DataFrame(data.reshape(-1,4))
        self.df.columns= ['vid','seq','t','y']        
        self.df= self.df.set_index( ['vid','t'] )
        del self.df['seq']
                
        ### Cache data
        self.df.to_pickle(self.vcache)
    
    def get_vector(self, vectorID ):
        """ Return data serie of measured data (time vector)"""
        
        return self.getDataFrame().xs(vectorID)
        