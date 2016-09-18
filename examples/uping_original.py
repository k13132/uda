from uda.loaders.flowping import FlowPing
from uda.loaders.flowping.readers import original
from pylab import *
from datetime import datetime

# Vstupni data
#fc='./examples/4mc-mereni-uping-O2-klient-10_1M.uping'
#fs='./examples/4mc-mereni-uping-O2-server-10_1M.uping'

fc='d:\\tmp\\2014.06.18_UDA.tests\\test_noE.noC.fpc'
fs='d:\\tmp\\2014.06.18_UDA.tests\\test_noE.noC.fps'

fc='d:\\tmp\\2014.06.18_UDA.tests\\test_noE.C.fpc'
fs='d:\\tmp\\2014.06.18_UDA.tests\\test_noE.C.fps'


### Nacteni a zabrazeni surovych dat
t0= datetime.now()
up= FlowPing(server_log=fs, client_log=fc, gzip=False, ftype="csv")
df= up.getDataFrame()
t1= datetime.now()
print 'Doba nutna ke zpracovani dat:', t1-t0

# Zobrazeni
figure(1, figsize=(16,10))
suptitle('Surove data')

subplot(231)
title('Server delta')
df['server']['delta'].plot(marker='.', linestyle=' ')

subplot(232)
title('Server delta')
hist( df['server', 'delta'], bins=50 )

subplot(233)
title('Server loss indicator')
df['server','loss_indicator'].plot()


subplot(234)
title('Client RTT')
df['client','rtt'].plot(marker='.', linestyle=' ')

subplot(235)
title('Client RTT')
hist( df['client','rtt'], bins=50 )

subplot(236)
title('Client loss indicator')
df['client','loss_indicator'].plot()
show()


### Nacteni a prumerovani dat
t0= datetime.now()
up= FlowPing(server_log=fs, client_log=fc, gzip=False, reader=original.FlowPingReaderInterval, window='1s', ftype="csv")
#up= uPing(server_log=fs, client_log=fc, gzip=False, reader=original.uPingReaderInterval, window='1s', how='mean')
df= up.getDataFrame()
t1= datetime.now()
print 'Doba nutna ke zpracovani dat:', t1-t0

# Zobrazeni
figure(2, figsize=(16,10))
suptitle('Prumerovana data na intervalu 1s')

subplot(231)
title('Server delta')
df['server']['delta'].plot(marker='.', linestyle=' ')

subplot(232)
title('Server delta')
hist( df['server', 'delta'], bins=50 )

subplot(233)
title('Server packet loss')
df['server','packet_loss'].plot()


subplot(234)
title('Client RTT')
df['client','rtt'].plot(marker='.', linestyle=' ')

subplot(235)
title('Client RTT')
hist( df['client','rtt'], bins=50 )

subplot(236)
title('Client packet loss indicator')
df['client','loss_indicator'].plot()
show()
