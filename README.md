# Universal Data Analyzer

Universal Data Analyzer (UDA) emerged in 2014 from the need to statistically evaluated data create by OMNeT++. Evaluation of real-world measurements carried out by [FlowPing](https://github.com/k13132/flowping) was required later. That time modular architecture was designed and implemented. Currently, UDA's public functions allow evaluation of OMNeT++ and FlowPing measurements.

Processed files are returned in [DataFrame](http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html) format for further analysis.

Current OMNeT++ modul can read .sca, .vec, and .vci files. Histogram measurements are on TODO. 

## Getting started

In order to use UDA, required Python libraries need to be installed. Then UDA data loaders can be used. For example, OMNeT++ project loader:

```python
from uda.loaders.omnetpp import Omnetpp

o= Omnetpp(SIMNAME, RUNID)
vec_df_= o.vec.getDataFrame()
vci_df_= o.vci.getDataFrame()
sca_df_= o.sca.getDataFrame()
```

More sofisticated analysis using OMNeT++ loader can be seen in [Examples](examples/omnetpp.py)

## ToDo

* Implementation of OMNeT++ histogram format