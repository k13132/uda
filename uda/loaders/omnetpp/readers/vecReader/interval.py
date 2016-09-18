# -*- coding: utf-8 -*- 

import uda
import numpy as np
from base_interval import VecReader as BaseIntervalVecReader
from uda.extra import hypo_linregres

class VecReader(BaseIntervalVecReader):
    
    def col_linregres(self,vid=None,x=None,y=None, heading=False):
        """ 
        Hypothesis testing of relation between x and y   
        H0 - x does not influence y (flat in time= stationary)
        H1 - x does influence y    
        """
        if heading: return ('lr_slope','lr_intercept','lr_p','lr_r','lr_err', 'lr_crit')
        return hypo_linregres(x,y)
    
    
    def col_mean(self,vid=None,x=None,y=None, heading=False):
        if heading: return ('mean',)
        return np.mean(y)

    def col_median(self,vid=None,x=None,y=None, heading=False):
        if heading: return ('median',)
        return np.median(y)  

    def col_min(self,vid=None,x=None,y=None, heading=False):
        if heading: return ('min',)
        return np.min(y)  
    
    def col_max(self,vid=None,x=None,y=None, heading=False):
        if heading: return ('max',)
        return np.max(y)  
    
    def col_length(self,vid=None,x=None,y=None, heading=False):
        if heading: return ('n',)
        return x.__len__()
