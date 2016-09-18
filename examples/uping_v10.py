from uda.loaders.flowping import FlowPing
from uda.loaders.flowping.readers import v10
from pylab import *
from datetime import datetime

# Vstupni data
fs='./examples/4mc-mereni-uping-O2-server-v3-7.txt'
fc='./examples/4mc-mereni-uping-O2-klient-v3-7.txt'


### Nacteni a zabrazeni surovych dat
t0= datetime.now()
up= FlowPing(server_log=fs, client_log=fc, gzip=False, reader=v10.FlowPingReader)
df= up.getDataFrame()
t1= datetime.now()
print 'Doba nutna ke zpracovani dat:', t1-t0

# Zobrazeni
figure(1, figsize=(16,10))
suptitle('Surove data')

subplot(241)
title('Server delta')
df['server']['delta'].plot()

subplot(242)
title('Server delta')
hist( df['server', 'delta'], bins=50 )

subplot(243)
title('Server loss indicator')
df['server','loss_indicator'].plot()

subplot(244)
title('Server RX/TX')
df['server','rx'].plot(label='rx')
df['server','tx'].plot(label='tx')
legend(loc='Best')


subplot(245)
title('Client time')
df['client','time'].dropna().plot()

subplot(246)
title('Client time')
hist( df['client','time'].dropna() )

subplot(247)
title('Client loss indicator')
df['client','loss_indicator_from'].dropna().plot(label='from')
df['client','loss_indicator_to'].dropna().plot(label='to')
legend(loc='Best')

subplot(248)
title('Client RX/TX')
df['server','rx'].dropna().plot(label='rx')
df['server','tx'].dropna().plot(label='tx')
legend(loc='Best')
show()



### Nacteni a prumerovani dat
t0= datetime.now()
up= FlowPing(server_log=fs, client_log=fc, gzip=False, reader=v10.FlowPingReaderInterval, window='1s')
#up= uPing(server_log=fs, client_log=fc, gzip=False, reader=v10.uPingReaderInterval, window='1s', how='mean')
df= up.getDataFrame()
t1= datetime.now()
print 'Doba nutna ke zpracovani dat:', t1-t0

# Zobrazeni
figure(2, figsize=(16,10))
suptitle('Prumerovana data na intervalu 1s')

subplot(241)
title('Server delta')
df['server']['delta'].plot()

subplot(242)
title('Server delta')
hist( df['server', 'delta'], bins=50 )

subplot(243)
title('Server packet loss')
df['server','packet_loss'].plot()

subplot(244)
title('Server RX/TX')
df['server','rx'].plot(label='rx')
df['server','tx'].plot(label='tx')
legend(loc='Best')



subplot(245)
title('Client time')
df['client','time'].dropna().plot()

subplot(246)
title('Client time')
hist( df['client','time'].dropna(), bins=50 )

subplot(247)
title('Client packet loss')
df['client','packet_loss_from'].dropna().plot(label='from')
df['client','packet_loss_to'].dropna().plot(label='to')
legend(loc='Best')

subplot(248)
title('Client RX/TX')
df['client','rx'].dropna().plot(label='rx')
df['client','tx'].dropna().plot(label='tx')
legend(loc='Best')
show()
