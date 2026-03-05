#
# All Copyright (c) 2023-2023, Sunqk Development Group
#

include ../Makefile.global

RIPPLE_SOURCE = $(wildcard *.c)
RIPPLE_OBJS = $(patsubst %.c, %.o, $(RIPPLE_SOURCE))

TARGET_FILE = test_hash test_list test_stringinfo test_splitrecord test_parser
# 编译选项

all:${RIPPLE_OBJS} ${TARGET_FILE}

${RIPPLE_OBJS}:%.o:%.c
	${CC} ${RIPPLE_DEBUGFLAG} ${RIPPLE_CFLAGS} -L${RIPPLE_LIB} -L${DATABASE_LIB} -l${RIPPLE_NAME}  -lpq ${INCL} -c $< -o $@

test_hash:test_hash.o
	${CC} ${INCL} ${RIPPLE_DEBUGFLAG} ${RIPPLE_CFLAGS} -L${RIPPLE_LIB} -L${DATABASE_LIB} -l${RIPPLE_NAME} -l${PARSER_NAME} -lpq $< -o test_hash

test_list:test_list.o
	${CC} ${INCL} ${RIPPLE_DEBUGFLAG} ${RIPPLE_CFLAGS} -L${RIPPLE_LIB} -L${DATABASE_LIB} -l${RIPPLE_NAME} -l${PARSER_NAME} -lpq $< -o test_list

test_stringinfo:test_stringinfo.o
	${CC} ${INCL} ${RIPPLE_DEBUGFLAG} ${RIPPLE_CFLAGS} -L${RIPPLE_LIB} -L${DATABASE_LIB} -l${RIPPLE_NAME} -l${PARSER_NAME} -lpq $< -o test_stringinfo

test_splitrecord:test_splitrecord.o
	${CC} ${INCL} ${RIPPLE_DEBUGFLAG} ${RIPPLE_CFLAGS} -L${RIPPLE_LIB} -L${DATABASE_LIB} -l${RIPPLE_NAME} -l${PARSER_NAME} -lpq $< -o test_splitrecord

test_parser:test_parser.o
	${CC} ${INCL} ${RIPPLE_DEBUGFLAG} ${RIPPLE_CFLAGS} -L${RIPPLE_LIB} -L${DATABASE_LIB} -l${RIPPLE_NAME} -l${PARSER_NAME} -lpq $< -o test_parser

clean:
	rm -f ${RIPPLE_OBJS}
	rm -f ${TARGET_FILE}
