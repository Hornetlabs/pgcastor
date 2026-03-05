#
# All Copyright (c) 2024-2024, Byte Sync Development Group
#

include ../../Makefile.global
RIPPLE_SOURCE = $(wildcard *.c)
RIPPLE_OBJS = $(patsubst %.c, %.o, $(RIPPLE_SOURCE))


all:${RIPPLE_OBJS} testinterfaces ok

${RIPPLE_OBJS}:%.o:%.c
	${CC} ${RIPPLE_CFLAGS} ${RIPPLE_INCL} ${RIPPLE_INTERFACEINCL} -c $< -o $@

testinterfaces:${RIPPLE_OBJS}
	${CC} ${RIPPLE_CFLAGS} ${RIPPLE_INCL} ${RIPPLE_INTERFACEINCL} -Wl,-rpath,../../interfaces/lib ${XSCSCI_OBJ} -o testinterfaces ${RIPPLE_OBJS} -L${RIPPLE_INTERFACELIBDIR} -lxsynch


ok:
	rm -f ${RIPPLE_OBJS}
	@echo "-------------------------------------------" 
	@echo "    |      make testinterfaces success        | "
	@echo "-------------------------------------------" 
clean:
	rm -f ${RIPPLE_OBJS}
	rm -f testinterfaces

