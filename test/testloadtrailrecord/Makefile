#
# All Copyright (c) 2024-2024, Byte Sync Development Group
#

include ../../Makefile.global
RIPPLE_SOURCE = $(wildcard *.c)
RIPPLE_LIBOBJS = $(wildcard ../../lib/*.o)
RIPPLE_OBJS = $(patsubst %.c, %.o, $(RIPPLE_SOURCE))


all:${RIPPLE_OBJS} testloadtrailrecord ok

${RIPPLE_OBJS}:%.o:%.c
	${CC} ${RIPPLE_CFLAGS} ${RIPPLE_INCL} -c $< -o $@

testloadtrailrecord:${RIPPLE_OBJS}
	${CC} ${RIPPLE_CFLAGS} ${RIPPLE_INCL} $< -o testloadtrailrecord -L${RIPPLE_LIBDIR} ${RIPPLE_LIBOBJS} -L${RIPPLE_DB_LIBDIR} ${RIPPLE_DB_LIB} ${RIPPLE_OS_LIB} -L${RIPPLE_CURL_LIBDIR} ${RIPPLE_CURL_LIB}


ok:
	rm -f ${RIPPLE_OBJS}
	@echo "-------------------------------------------" 
	@echo "    |      make testloadtrailrecord success        | "
	@echo "-------------------------------------------" 
clean:
	rm -f ${RIPPLE_OBJS}
	rm -f testloadtrailrecord

