# -*- coding: utf-8 -*-
"""
Created on Fri Dec 13 14:53:51 2013

@author: milos
"""


class BaseReader(object):
    
    fpos = 0
    config = None
    df= None
    
    def getDataFrame(self):
        """ This function returns loaded data """
        
        # Load data if are not loaded
        if self.df is None:
            self._load_data()
        
        # Return data as uda.DataFrame
        return self.df
    
    
    
    

class ConfigReaderMixin(BaseReader):   
    def _read_config(self):
        """
        Class ConfigReaderMixin can only read config file header therefore must be inherited by all Readers
        
        """
        linenn, config= 0, {}
        for line in self.fp:
            # Capture current possition in file
            self.fpos += len(line)
            linenn += 1
            
            # Find the first empty line == end of header
            if line.strip() == '' and linenn > 1 : break
            _line= line.strip().split(' ')
            
            if _line[0] == 'file': continue
            
            # Store in the memory            
            id= _line[0] == 'attr' and  1 or 0
                 
            # Parse config lines
            if _line[id+1:].__len__() == 1:
                try:
                    config[ _line[id] ] = float(_line[id+1])
                except:
                    config[ _line[id] ] = _line[id+1]
            else:
                # Value is a set of variables
                _out= {}      
                for item in _line[id+1:]:
                    key, val = item.split('=')
                    key= key.replace('"',"")
                    val= val.replace(',',"")
                    val= val.replace('"',"")
                    try:
                        val= float(val)
                    except:
                        pass
                    
                    _out[ key ] = val
                config[ _line[id] ] = _out
        
        # Store the config values to the main config
        self.config= config
        
        
class VectorNameReaderMixin(BaseReader):
    """
    Class VectorNameReaderMixin only walks through given file from self.fp and scans it for vector names. It instantiates
    a new file pointer in order not to change possition in the file..
    """    
    
    # container of (vid, mod, var) for all/filtered vectors
    vectors_names= None
    
    def _load_vector_names(self):
        
        f= self.fp.name
        vecto= {}
        
        with open(f, "r") as fp:
            for line in fp:
                _data= line.split('\t')
                try:
                    # Read the Vector ID
                    vid= int(_data[0])
                except:
                    line= line.split(' ')
                                        
                    if line[0] != 'vector': continue 
                    
                    vid, mod= int(line[1]), line[3].strip()
                    tp= line[-1].strip()
                    var=  ' '.join(line[4:-2]).replace('"','').strip()
                    
                    if not mod in vecto: vecto[mod]= {}
                    vecto[mod][var]= vid    
                                        
                    
                    # Add vid to the set of vectors
                    self.vectors.append(vid)
                    self.vectors_names.append( (vid, mod, var, tp) )
                    
        return self.vectors_names 
        
    def get_vector_names(self):
        """ Getter for vector_names """
        if self.vectors_names == None:
            self.vectors_names= []
            self._load_vector_names()
        return self.vectors_names
