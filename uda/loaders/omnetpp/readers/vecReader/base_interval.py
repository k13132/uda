# -*- coding: utf-8 -*- 

import os
import uda
import numpy as np
from bulk import VecReader as BulkVecReader
from uda.extra import hypo_linregres

INTERVAL=100
CACHE_INTERVAL=100

_m= {'l':np.min, 'r': np.max, 'c': np.mean }

class VecReader(BulkVecReader):
    
    INTERVAL= INTERVAL
    _x_method = 'c'
    parameters= {}
    
    def _DataFrameHeadings(self):
        """ This function automatically obtain headings of functions 
        that were aplied on given data. """
         
        _h= ['vid','x',]
        for func in filter( lambda x: 'col' in x, dir(self) ):
            func= getattr(self,func)
            _h += func(heading=True)
        
        # Retur headings
        return _h
    
    def _DataFrameLine(self,vid,x,y):
        """ Function that applies col_xxx(x,y) functions onto loaded interval of data """
        _o= []
        for func in filter( lambda x: 'col' in x, dir(self) ):
            func= getattr(self,func)
            _result= func(vid,x,y)
            if _result.__class__.__name__ != 'tuple':
                _result= [_result, ]
            _o+= _result
        
        # There you go
        return _o
        
    def _load_data(self):
        print 'Loading data...'
        
        # Cache
        _tmp,_cache={},{}
        for v in self.vectors: _tmp[v]= { 'cache': list(), 'values': np.array([]), 'len':0 }
        for v in self.vectors: _cache[v]= {'data':list(), 'len':0}
        _df, _tdf= np.array([]), []
        
        # Open the .vec file for reading
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
            
            # Parse data
            try:
                t= float(_data[2])
            except:
                print line
            
            ## Time sub-interval
            # Lower bound
            if t<self.Tmin: continue

            # Upper bound - T max allignment                    
            if t>self.Tmax:
                self._end_vectors.add(_cvid)
                if len(self._end_vectors) >= self._vectors_len:
                    eof=True
                    break
                continue
            
            # Cache data
            _cache[_cvid]['data'].append( (t, float(_data[3]) ) )
            _cache[_cvid]['len']+=1 
            
            # Aggregate lines
            if _cache[_cvid]['len'] >= self.INTERVAL:
                # Raw interval data
                x= [ i[0] for i in _cache[_cvid]['data'] ] 
                y= [ i[1] for i in _cache[_cvid]['data'] ]
                
                # Data wrangling operations
                _line= self._DataFrameLine(_cvid, x, y)
                _line.insert( 0,  _m.get(self._x_method, 'c' )(x) )
                _line.insert( 0, _cvid)
                
                _cache[_cvid]['len']= 0
                _cache[_cvid]['data']= list()
                
                _tdf.append( _line )
                if _tdf.__len__() >= CACHE_INTERVAL:
                    _df= np.append( _df, _tdf )                    
                    _tdf= []
        
        
        for _cvid in [ x for x in _cache if _cache[x]['len'] > 0  ]:
            x= [ i[0] for i in _cache[_cvid]['data'] ] 
            y= [ i[1] for i in _cache[_cvid]['data'] ]
                
            # Data wrangling operations
            _line= self._DataFrameLine(_cvid, x, y)
            _line.insert( 0,  _m.get(self._x_method, 'c' )(x) )
            _line.insert( 0, _cvid)
                
            _cache[_cvid]['len']= 0
            _cache[_cvid]['data']= list()
                
            _tdf.append( _line )
        
        if _tdf.__len__() > 0:
            _df= np.append( _df, _tdf )
            del _tdf
        
        ### Data are safely saved and converted to the internal structure
        _heading= self._DataFrameHeadings()
        df= uda.DataFrame( _df.reshape(-1,len(_heading)), columns=_heading )
        self.df= df.set_index( ['vid','x'] )

    def get_vector(self, vectorID ):
        """ Return data serie of measured data (time vector)"""
        df= self.getDataFrame()
        if not vectorID in df.index.levels[0]:
            return None
        return df.xs(vectorID)