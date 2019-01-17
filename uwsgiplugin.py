import os

NAME = 'stats_pusher_mongodb'

CFLAGS = [
    '-std=c++11',
    '-Wno-error',
]
LDFLAGS = []
LIBS = []


def split_flags(s):
    flags = s.rstrip().split()
    return [] if flags == [''] else flags


def pkgconfig_flags(switch):
    return split_flags(os.popen('pkg-config --%s libmongoc-1.0' % switch).read())


CFLAGS += pkgconfig_flags('cflags')
LIBS = pkgconfig_flags('libs-only-l')
LDFLAGS = pkgconfig_flags('libs-only-L')

GCC_LIST = ['plugin.cc', 'transform_metrics.cc']
