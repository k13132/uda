# -*- coding: utf-8 -*- 

import os
from readers import *
import numpy as np


VECREADER= {'simple':simple,
            'bulk':bulk,
            'afinterval':afinterval,
            'interval':interval}

class Omnetpp(object):
    """ Plain OMNeT++ loader """
        
    # Simulation name
    simname= None

    # Identifier of the analysed run
    run = None
    
    # Data files
    vec,  sca, vci= None, None, None
    _vec,_sca,_vci= None, None, None
    
    # Available vectors
    parameters= None
    
    def __init__(self, simname, runid, root='results', tmin=0, tmax=float("inf"),vectors=None, vecreader='simple'):
        ### Data files
        # .vci
        try:
            self._vci = os.path.join(root, '%s-%d.vci'%(simname, runid) )
            self.vci = VciReader( self._vci ) 
            self.parameters= self.vci.parameters
            # Filtering set of requested vectors
            self.vectors= self.vci.vectors
            self.config= self.vci.config
            if not vectors is None:
                self.vectors= filter( lambda x:x in vectors, self.vci.vectors )
            fallback= False
        except Exception, e:
            print '.VCI does not exists - fall back mode: "%s"'%e
            self.vectors= []
            fallback= True
            self.vci = None
       
        # .vec 
        if not vecreader is None:
            try:
                self._vec = os.path.join(root, '%s-%d.vec'%(simname, runid) )

                if vecreader.__class__.__name__ == 'str':                
                    self.vec= VECREADER.get(vecreader, 'simple').VecReader( self._vec, tmin=tmin, tmax=tmax, vectors=self.vectors )
                else:
                    self.vec= vecreader( self._vec, tmin=tmin, tmax=tmax, vectors=self.vectors )
                    
                if fallback:
                    self.parameters= self.vec.parameters
                    self.config= self.vec.config
                    print '.VEC exists! But things are going to be slow!!! '
            except Exception, e:
                if fallback:
                    raise Exception('There is something very wroing happening. Evaluation stopped !!! %s'%e)

        
        # .sca
        try:
            self._sca = os.path.join(root, '%s-%d.sca'%(simname, runid) )
            self.sca = ScaReader( self._sca ) 
        except:
            print '.SCA does not exists, really? It looks like a big problem!'
            self.sca= None
            
            if fallback:
                raise Exception('There are no data!')
    
    def set_vectors(self, vectors):
        self.vectors= vectors
        self.vec.vectors= vectors
        self.vci.vectors= vectors

    def get_vectors_names(self):
        try:
            return self.vci.vectors_names
        except:
            return self.vec.get_vector_names()
        
    def vector(self,vectorID, ts=False):
        v= self.vec.get_vector( vectorID )
        if ts: return (v.index, v.values)
        return v
        
        
class AutoFocusOmnetpp(Omnetpp):
    
    SAMPLES_1S= 100
    
    def __init__(self, *args, **kwargs):
        """ Initialization  """
        
        # Call init of SimpleVecReader .. then extra __init__ can be done..
        kwargs['vecreader']= 'afinterval'
        super( AutoFocusOmnetpp, self ).__init__(*args, **kwargs)
        
        # INTERVAL update for module interval
        max_time= 0.0
        max_intervals=0
        min_intervals=0
        try:
            vdf= self.vci.getDataFrame()
            max_time= vdf['lastSimtime'].max()
            max_intervals= int( vdf.reset_index().groupby('vid').sum()['count'].max() )
            min_intervals= int( vdf.reset_index().groupby('vid').sum()['count'].min() )
        except:
            max_time, min_intervals, max_intervals = self.vec.get_properties()
        
        # Samples in one interval (one interval is one point of graph)
        interval= int( max_intervals / ( self.SAMPLES_1S*max_time) )
            
        print 'Updating INTERVAL to', interval
        print max_time
        print max_intervals
        print min_intervals
        
        self.vec.set_interval(interval)
            
class VectorFilter(object):

    def __init__(self, oobject=None ):
        self.o= oobject
        self.filters= []  
        self.filters_all= []
    
    
    def _filter(self, vects=None, **args):
        """" Elementary function that filters vects according to the given args (var,mod) """
        
        if vects is None:
            if self.o is None:
                raise Exception('No OMNeT++ module to be filtered')            
            vects= self.o.get_vectors_names()
        
        if 'mod' in args:
            vects= filter( lambda x:args["mod"] in x[1], vects )
        
        if 'var' in args:        
            vects= filter( lambda x:args["var"] in x[2], vects )
        
        return vects
    
    
    filters_all= []
    def addFilter(self, **args):
        self.addFilterAND(**args)
        
    def addFilterAND(self, **args):
        self.filters.append( args )
    
    def addFilterOR(self, **args):
        
        if self.filters.__len__() > 0:
            self.filters_all.append( self.filters )
            self.filters= []
        
        if args.__len__() > 0:
            self.filters.append( args )
    
    def _filter2(self, *args):  

        # 
        if self.o is None:
            
            if args.__len__() == 0:
                raise Exception('No OMNeT++ module to be filtered')
            
            self.o= args[0]
        
        # Append some missing filters        
        self.filters_all.append( self.filters )
        
        # Hierarchical filter
        vects_final =[]    
        for ff in self.filters_all:
            vects= self.o.get_vectors_names()
            for f in ff:
                vects= self._filter(vects, **f)
            vects_final+= vects
        
        # Polish to be unique
        mapa= {}
        for x in vects_final:
            mapa[ x[0]] =x
        
        # Return unique data
        return mapa.values()
        

    def filter(self, *args, **kargs):  
        """ Interface function to be called """        
        
        # The extended filterings
        if kargs.__len__() == 0:
            return list(set([ x[0] for x in self._filter2(*args) ]))
        
        # Basic filtering
        return list(set([ x[0] for x in self._filter(**kargs) ]))

    def show(self, *args, **kargs):  
        """ Interface function to be called """        
        
        # The extended filterings
        if kargs.__len__() == 0:
            results= self._filter2(*args)
        
        # Basic filtering
        else:
            results= self._filter(**kargs)
            
        mapa={}
        for x in results:
            mapa[ x[0] ] = x
            
        for x in mapa.values() :
            print x


class BatchOmnetpp(Omnetpp):
    
    runs= None
    
    def __init__(self, simname, runs=None, **kwargs):
        
        ## Runs
        if runs is None:
            raise Exception('Runs must be defined as a list of IDs')
        self.runs= runs
        
        print kwargs
        pass
    
    
    