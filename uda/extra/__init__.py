# -*- coding: utf-8 -*- 
import numpy as np
from scipy import stats

def hypo_linregres(x,y):
    """ 
    Hypothesis testing of relation between x and y   
    H0 - x does not influence y (flat in time= stationary)
    H1 - x does influence y    
    """
    
    # Vector for results
    _res= []
    
    # Analysis
    try:
        b1, b0, r_value, p_value, stdErr = stats.linregress(x, y)
        _res+= [b1, b0, r_value, p_value, stdErr, np.NAN ]
        
        # Testing the hypothesis
        # Other parameters
        n= len(y)
        sy= sum(y)
        sy2= sum( [ i*i for i in y ] )
        sxy= sum( np.array(x)*np.array(y) )
        s2= float(sy2 - b0*sy - b1* sxy)/(n-2)
        if s2==0.0: return tuple(_res)
            
        # Evaluation of critical value
        mxi= np.mean(x)
        sx2= sum( [ i*i for i in x ] )
        crit= np.sqrt( float(sx2 - n*mxi*mxi ) / s2 )*abs(b1)
        
        """
        # This is how to very flatness
        from scipy.stats import t
        if crit < t.ppf(1-ALPHA/2, n-2):
            H0_is_stationary= True
        """
        _res[-1]= crit
        
    #Something went wrong
    except Exception, e:
#        printz e 
        pass
    
    return tuple(_res)


