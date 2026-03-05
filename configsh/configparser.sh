#!/bin/bash
#
# All Copyright (c) 2024-2024, Byte Sync Development Group
# 生成 makefile 文件
#
function configParserMKGlobal
{
    if [ ${RIPPLE_OP} == "clear" ]
    then
        rm -f ${RIPPLE_BASE}/parser/Makefile.global
        return
    fi

    echo "#"                                                                    > ${RIPPLE_BASE}/parser/Makefile.global
    echo "# All Copyright (c) 2024-2024, Byte Sync Development Group"           >> ${RIPPLE_BASE}/parser/Makefile.global
    echo "#"                                                                    >> ${RIPPLE_BASE}/parser/Makefile.global
    echo "MODULE_PATH = ${RIPPLE_BASE}/parser"                                  >> ${RIPPLE_BASE}/parser/Makefile.global
    echo "MODULE_SRC = ${RIPPLE_BASE}/parser/src"                               >> ${RIPPLE_BASE}/parser/Makefile.global
    echo "MODULE_INCL = ${RIPPLE_BASE}/parser/include"                          >> ${RIPPLE_BASE}/parser/Makefile.global
    echo "LIBPATH = ${RIPPLE_BASE}/lib"                                         >> ${RIPPLE_BASE}/parser/Makefile.global
    echo "DATABASE_INCL=${DATABASE_DIR}/include"                                >> ${RIPPLE_BASE}/parser/Makefile.global
    echo "DATABASE_LIB=${DATABASE_DIR}/lib"                                     >> ${RIPPLE_BASE}/parser/Makefile.global
    echo "SHARED_LIB_FLAG = -shared"                                            >> ${RIPPLE_BASE}/parser/Makefile.global
    echo "FPICFLAG = -fPIC"                                                     >> ${RIPPLE_BASE}/parser/Makefile.global
    echo "CFLAGS = ${RIPPLE_CFLAGS}"                                            >> ${RIPPLE_BASE}/parser/Makefile.global
    echo "CC = gcc "                                                             >> ${RIPPLE_BASE}/parser/Makefile.global
}
