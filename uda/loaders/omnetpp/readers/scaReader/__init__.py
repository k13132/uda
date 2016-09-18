import uda
from uda.loaders.omnetpp.readers.base import ConfigReaderMixin

class ScaReader(ConfigReaderMixin):
    
    def __init__(self, sca ):        
        self.sca= sca
        self.fp= open(sca, 'r')        
        
        # Read the config file
        self._read_config()

    
    def _load_data(self):
        """ Load scalar data """         
        _out = {}
        for line in self.fp:
            # Filter out only the scalar lines
            if 'attr' in line: continue
                
            a,b,c= line.split('\t')
            
            # Data converting
                
            a= a.replace('scalar','').strip()
            b=b.replace('"','').strip()
            c= float(c)
            if a=='.': a='general'
            
            # Saving
            if not a in _out: _out[a]= {}
            _out[a][b]= c
        
        # Return tha uda.DataFrame
        self.df= uda.DataFrame(_out)