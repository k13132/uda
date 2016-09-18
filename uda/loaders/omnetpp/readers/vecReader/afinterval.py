'''
Created on 27.1.2014

@author: milos
'''

import os
import uda
import numpy as np
from interval import VecReader as IntervalVecReader

class VecReader(IntervalVecReader):
    
    def get_properties(self):
        max_t= 0.0
        count= {}
        
        self._fp_vec= open(self.vec, 'r')
        self._fp_vec.seek(self._data_start_tell)
        
        # Walk through the file
        for line in self._fp_vec:
            
            _data= line.split('\t')
            try:
              _cvid= int( _data[0] )
              if not _cvid in count: count[_cvid]=0
              count[_cvid]+=1
              _t= float(_data[2])
              if _t > max_t: max_t= _t
            except:
                continue
        
        self._fp_vec.close()  
            
        # Return vector description        
        return max_t, min( count.values() ), max( count.values() )
    
    def set_interval(self, interval):
        self.INTERVAL= interval