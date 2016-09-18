from uda.loaders.flowping import FlowPing
from uda.loaders.flowping.readers import v12
from pylab import *
from datetime import datetime

# Vstupni data
fs='flowping-csv-sample/milos-sample-server.csv'
fc='flowping-csv-sample/milos-sample-client.csv'

t0= datetime.now()
up= FlowPing(server_log=fs, client_log=fc, gzip=False, reader=v12.FlowPingReader)
df= up.getDataFrame()
t1= datetime.now()
print 'Doba nutna ke zpracovani dat:', t1-t0


# Zobrazeni
figure(1, figsize=(16,10))
suptitle('Surove data')

subplot(231)
title('Server delta')
df['server']['delta'].plot()

subplot(232)
title('Server delta')
hist( df['server', 'delta'], bins=50 )

subplot(233)
title('Server loss indicator')
df['server','loss_indicator'].plot()

subplot(234)
title('Server RX/TX')
df['server','rx'].plot(label='rx')
df['server','tx'].plot(label='tx')
legend(loc='Best')


subplot(235)
title('Client loss indicator')
df['client','loss_indicator_from'].dropna().plot(label='from')
df['client','loss_indicator_to'].dropna().plot(label='to')
legend(loc='Best')

subplot(236)
title('Client RX/TX')
df['server','rx'].dropna().plot(label='rx')
df['server','tx'].dropna().plot(label='tx')
legend(loc='Best')
show()