'''
Created on 20.5.2013

@author: Ihi
'''

import calendar
import datetime
import dateutil.parser
import pytz

# Convert a unix time u to a datetime object d, and vice versa
def ut(d):
    return calendar.timegm(d.timetuple())

def linux_to_iso(linuxTime):
    return datetime_to_iso(linux_to_datetime(linuxTime))

def iso_to_linux(isoTime):
    return datetime_to_linux(iso_to_datetime(isoTime))
   
def linux_to_datetime(linuxTime):
    return datetime.datetime.utcfromtimestamp(float(linuxTime))

def iso_to_datetime(isoTime):
    return dateutil.parser.parse(isoTime)

def datetime_to_linux(dt):
    return ( calendar.timegm(dt.timetuple()) + (dt.microsecond / 1000000.) )
#    return ((time.mktime(dt.timetuple())) + (dt.microsecond / 1000000.) )

def datetime_to_iso(dt):
    return dt.isoformat()

def datetime_add_UTC(dt):
    return pytz.utc.localize(dt, is_dst=True)

def datetime_convert_to_UTC(dt):
    if dt.tzinfo == None:
        return datetime_add_UTC(dt)
    return pytz.utc.normalize(dt, is_dst=True)

def string_TSU(dt):
    return "{:.6f}".format(datetime_to_linux(dt))

def string_TSI(dt):
    return datetime_to_iso(dt)

def filenamize(filename):
    import string
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    valid_chars = frozenset(valid_chars)
    return ''.join(c for c in filename if c in valid_chars)



#if __name__ == '__main__':
#    dtnow = datetime.datetime.now()
#    print dtnow
#    lt = datetime_to_linux(dtnow)
#    it = datetime_to_iso(dtnow)
#    print linux_to_datetime(lt)
#    print iso_to_datetime(it)