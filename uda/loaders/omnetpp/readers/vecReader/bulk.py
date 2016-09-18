# -*- coding: utf-8 -*- 

import os
import uda
import numpy as np
from simple import VecReader as SimpleVecReader
INTERVAL=100
CACHE_FLUSH= 2e9

class VecReader(SimpleVecReader):
    """ Class that only reads .vec files of OMNeT++. 
        Returns DataFrame of data, empty spaces are replaced by NA
        """
        
    _vectors_len= 0
    
    def __init__(self, *args, **kwargs):
        """ Initialization of .vec readeru """
        
        # Call init of SimpleVecReader .. then extra __init__ can be done..
        super( VecReader, self ).__init__(*args, **kwargs)

        # Initialize set of required vectors
        self._vectors_len= self.vectors.__len__()
        
        # Initialize headers
        self._get_data_start()
    
    _data_start_tell=0
    def _get_data_start(self):
        """ Finds begging of data in .vec file"""
        with open(self.vec,'r') as fp:
            for line in fp:
                try:
                    int(line.split('\t')[0])
                    break
                except:
                    self._data_start_tell += len(line)
    
    def _load_cache(self):
        if os.path.exists( self.vcache ):
            self.df= uda.read_csv(self.vcache)
            
            # Recreating index
            self.df.index= self.df['time [s]']
            del self.df['time [s]']
            
            # Columns renaming
            self.df.columns= map( lambda x:int(x), self.df.columns )
            
            # Do we need to load data all all required data are in cache?
            _have_got_cached= filter( lambda x:x in self.df.columns, self.vectors )
            if len(_have_got_cached) == len(self.vectors):   
                # Provide only required data
                for v in filter( lambda x:x not in self.vectors, self.df.columns ):
                    del self.df[v]
                
                # Lets continue with cached data
                return True
        return False
    
    _has_data_for_dataframe= True
    _fp_vec= None
    _x_method= 'c'
    _m= {'l':np.min, 'r': np.max, 'c': np.mean }
    _cache= {}
    _lines= 0
    
    #@profile
    def _load_data(self):
        ### Load data from .vec.cache file if exists and is correct
        if self._load_cache(): return
    
        ### Load data from .vec file
        self._fp_vec= open(self.vec, 'r')
        self._fp_vec.seek(self._data_start_tell)
        
        _empty_vector= set()
        _has_data_for_dataframe= True
        chunk=0
        
        # Initialize cache
        for vid in self.vectors:
            self._data_cache[vid]= []
        
        # Walking through the .vec file 
        while _has_data_for_dataframe:
            for vid in self.vectors:
            
                # Find the vector significance
                #if not self._vector_significant(vid): continue
                
                # Read the date for given interval
                x,y= self._vector_data(vid)
                
                # Calculate the index
                t= self._m.get(self._x_method, 'c' )(x)
                
                if x.__len__() == 0:
                    _empty_vector.add(vid)
                    #print 'prazdny', vid, _empty_vector, self.vectors, _empty_vector.__len__(), _vectors_len
                    
                    if _empty_vector.__len__() == self._vectors_len:
                        _has_data_for_dataframe= False
                        break
                    
                    continue
                
                # Store data to the cache
                try:
                    self._cache[vid][t] = float( np.mean(y) )
                except:
                    self._cache[vid] = {}
                    self._cache[vid][t] = float( np.mean(y) )
                    
                    
                # 
                if self._cache.__sizeof__() >= CACHE_FLUSH:
                    print 'Saving chunk %d ... lines checked %d'%(chunk,self._lines)
                    self.df= uda.DataFrame(self._cache)
                    self.df.index.name='time [s]'                    
                    self.df.to_csv("%s.%d"%(self.vcache,chunk) )
                    self._cache= {}
                    chunk += 1

        # Translate to Pandas DataFrame
        self.df= uda.DataFrame(self._cache)
        self.df.index.name='time [s]'
        
        ### Cache data
        if chunk==0: self.df.to_csv(self.vcache)
        else:
            self.df.to_csv("%s.%d"%(self.vcache,chunk) )
            with open(self.vcache, 'w') as fp:
                fp.write(chunk)
        
    _signi_data= {}
    def _vector_significant(self, vid):
        if not vid in self._cache:
            self._signi_data[vid]= 0
            return True
        
        self._signi_data[vid]= self._cache[vid].__len__()

        #print self._cache
#        print [ self._cache[i].__len__() for i in self._cache]
        total= sum([ self._cache[i].__len__() for i in self._cache]) 

        if self._signi_data[vid]/total < 0.1:
        #float(1)/self._vectors_len:
            return False
        
        return True
        
        
                        

    _data_cache= {}
    def _vector_data(self, vid):
        
        if not self._fp_vec.closed:
            # If there is not enought data, load them!
            if len(self._data_cache[vid]) < INTERVAL:       
                self._vector_cache(vid)
            
        # Reformat the data
        _d= self._data_cache[vid][:INTERVAL]
        x,y = map( lambda x:x[0], _d), map( lambda x:x[1], _d)
        del self._data_cache[vid][:INTERVAL]

        # Return formated data
        return x, y

    _end_vectors= set()
    _vid_tell_cache= {}
    def _vector_cache(self, vid):
        
        # Read current length
        _len= len(self._data_cache[vid])
        
        # walk the file
        eof=True
        for line in self._fp_vec:
            eof=False
            _data= line.split('\t')
            _cvid= int( _data[0] )
            self._lines += 1
            
            try:
                
                # Store only relevant data
                if not _cvid in self.vectors: continue

                # T min allignment
                t= float(_data[2])
                if t<self.Tmin: continue

                # T max allignment                    
                if t>self.Tmax:
                    self._end_vectors.add(_cvid)
                    if len(self._end_vectors) >= self._vectors_len:
                        eof=True
                        break
                    continue
                                
                # Buffer data    
                self._data_cache[_cvid].append( (t, float(_data[3])) )                             
                
                # Evaluate end of reading
                if _cvid==vid: _len+=1
                if _len >= INTERVAL: break
                
                
            except Exception, e:
                print 'error: %s on line:'%e, line

        # Close the file    
        if eof: self._fp_vec.close()

