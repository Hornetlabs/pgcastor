#!/bin/bash
#
# All Copyright (c) 2024-2024, Byte Sync Development Group
#.生成 bin 下的内容
#---------------------------------------------------------


function configXSCSCIMK
{
    cd $1
    configMKHead Makefile

    echo "include ${RIPPLE_BASE}/Makefile.global"                                       >> Makefile
    echo "RIPPLE_OBJS = xscsci_scansup.o \\"                                            >> Makefile
    echo "				xscsci_input.o \\"                                              >> Makefile
    echo "				xscsci_output.o \\"                                             >> Makefile
    echo "				xscsci_tabcomplete.o \\"                                        >> Makefile
    echo "				xscsci_prescan.o \\"                                            >> Makefile
    echo "				xscsci_gram.o \\"                                               >> Makefile
    echo "				xscsci_precommand.o \\"                                         >> Makefile
    echo ""                                                                             >> Makefile
    echo "XSCSCI_OBJ = xscsci.o"                                                        >> Makefile
    echo "XSCSCISCAN_LEX = xscsci_prescan.c xscsci_scan.c"                              >> Makefile
    echo "XSCSCISCAN_YACC = xscsci_gram.c"                                              >> Makefile
    echo ""                                                                             >> Makefile
    echo ""                                                                             >> Makefile
    echo "all:\${XSCSCISCAN_YACC} \${XSCSCISCAN_LEX} \${XSCSCI_OBJ} \${RIPPLE_OBJS} $2 ok"                                                    >> Makefile
    echo ""                                                                             >> Makefile
    echo "\${XSCSCISCAN_YACC}:%.c:%.y"                                                  >> Makefile
    echo "	bison -Wno-deprecated -d -o \$@ $<"                                                 >> Makefile
    echo ""                                                                             >> Makefile
    echo "\${XSCSCISCAN_LEX}:%.c:%.l"                                                   >> Makefile
    echo "	flex -Cfe -p -p -o'\$@' $<"                                                 >> Makefile
    echo ""                                                                             >> Makefile
    echo "\${RIPPLE_OBJS}:%.o:%.c"                                                      >> Makefile
    echo "	\${CC} \${RIPPLE_CFLAGS} \${RIPPLE_INCL} \${RIPPLE_INTERFACEINCL}  -c $< -o \$@"                      >> Makefile
    echo ""                                                                             >> Makefile
    echo "\${XSCSCI_OBJ}:%.o:%.c"                                                      >> Makefile
    echo "	\${CC} \${RIPPLE_CFLAGS} \${RIPPLE_INCL} \${RIPPLE_INTERFACEINCL} -c $< -o \$@"                      >> Makefile
    echo ""                                                                             >> Makefile
    echo "$2:\${XSCSCI_OBJ} \${RIPPLE_OBJS}"                                                           >> Makefile
    echo "	\${CC} \${RIPPLE_CFLAGS} \${RIPPLE_INCL} \${RIPPLE_INTERFACEINCL} -Wl,-rpath,../../interfaces/lib \${XSCSCI_OBJ} -o $2 \${RIPPLE_OBJS} -L\${RIPPLE_INTERFACELIBDIR} ${RIPPLE_INTERFACELIBNAME} -lreadline" >> Makefile
    echo ""                                                                             >> Makefile
    echo ""                                                                             >> Makefile
    echo "ok:"                                                                          >> Makefile
    echo "	rm -f \${RIPPLE_OBJS}"                                                      >> Makefile
    echo "	@echo \"-------------------------------------------\" "                     >> Makefile
    echo "	@echo \"    |      make $2 success        | \""                             >> Makefile
    echo "	@echo \"-------------------------------------------\" "                     >> Makefile
    echo "clean:"                                                                       >> Makefile
    echo "	rm -f xscsci_gram.h"                                                        >> Makefile
    echo "	rm -f \${XSCSCISCAN_YACC}"                                                  >> Makefile
    echo "	rm -f \${XSCSCISCAN_LEX}"                                                   >> Makefile
    echo "	rm -f \${RIPPLE_OBJS}"                                                      >> Makefile
    echo "	rm -f $2"                                                                   >> Makefile
    echo ""                                                                             >> Makefile
}

# $1 入参为生成 Makefile 的目录
# $2 可执行程序的名称
function configSpecialMK
{
    cd $1
    configMKHead Makefile

    echo "include ${RIPPLE_BASE}/Makefile.global"                                       >> Makefile
    echo "RIPPLE_SOURCE = \$(wildcard *.c)"                                             >> Makefile
    echo "RIPPLE_LIBOBJS = \$(wildcard ${RIPPLE_BASE}/lib/*.o)"                          >> Makefile
    echo "RIPPLE_OBJS = \$(patsubst %.c, %.o, \$(RIPPLE_SOURCE))"                       >> Makefile
    echo ""                                                                             >> Makefile
    echo ""                                                                             >> Makefile
    echo "all:\${RIPPLE_OBJS} $2 ok"                                                    >> Makefile
    echo ""                                                                             >> Makefile
    echo "\${RIPPLE_OBJS}:%.o:%.c"                                                      >> Makefile
    echo "	\${CC} \${RIPPLE_CFLAGS} \${RIPPLE_INCL} -c $< -o \$@"                      >> Makefile
    echo ""                                                                             >> Makefile
    echo "$2:\${RIPPLE_OBJS}"                                                           >> Makefile
    echo "	\${CC} \${RIPPLE_CFLAGS} \${RIPPLE_INCL} $< -o $2 -L\${RIPPLE_LIBDIR} \${RIPPLE_LIBOBJS} -L\${RIPPLE_DB_LIBDIR} \${RIPPLE_DB_LIB} \${RIPPLE_OS_LIB} -L\${RIPPLE_CURL_LIBDIR} \${RIPPLE_CURL_LIB}"  >> Makefile
    echo ""                                                                             >> Makefile
    echo ""                                                                             >> Makefile
    echo "ok:"                                                                          >> Makefile
    echo "	rm -f \${RIPPLE_OBJS}"                                                      >> Makefile
    echo "	@echo \"-------------------------------------------\" "                     >> Makefile
    echo "	@echo \"    |      make $2 success        | \""                             >> Makefile
    echo "	@echo \"-------------------------------------------\" "                     >> Makefile
    echo "clean:"                                                                       >> Makefile
    echo "	rm -f \${RIPPLE_OBJS}"                                                      >> Makefile
    echo "	rm -f $2"                                                                   >> Makefile
    echo ""                                                                             >> Makefile
}

##bin 目录下删除Makefile
function configMKBinClear
{
    rm -f ${RIPPLE_BASE}/bin/capture/Makefile
    rm -f ${RIPPLE_BASE}/bin/integrate/Makefile
    rm -f ${RIPPLE_BASE}/bin/receivepglog/Makefile
    rm -f ${RIPPLE_BASE}/bin/xscsci/Makefile
    rm -f ${RIPPLE_BASE}/bin/ripple/Makefile
    rm -f ${RIPPLE_BASE}/bin/Makefile
}

##bin 目录生成
function configMKBin
{

    if [ ${RIPPLE_OP} == "clear" ]
    then
        configMKBinClear
        return
    fi

    cd ${RIPPLE_BASE}/bin
    local subdircnt=0
    local -a subdirs

    subdirs[$subdircnt]=${RIPPLE_BASE}/bin/capture
    let subdircnt+=1

    subdirs[$subdircnt]=${RIPPLE_BASE}/bin/integrate
    let subdircnt+=1

# recivelog begin

    subdirs[$subdircnt]=${RIPPLE_BASE}/bin/receivepglog
    let subdircnt+=1

# recivelog   end

# xscsci begin
    subdirs[$subdircnt]=${RIPPLE_BASE}/bin/xscsci
    let subdircnt+=1

# xscsci   end

# xmanager begin
    subdirs[$subdircnt]=${RIPPLE_BASE}/bin/xmanager
    let subdircnt+=1

# xmanager end
    configMKSubDirs "${subdirs[@]}"

    ##capture
    configSpecialMK ${RIPPLE_BASE}/bin/capture capture

    ##integrate
    configSpecialMK ${RIPPLE_BASE}/bin/integrate integrate

    ##pgreceivewal
    configSpecialMK ${RIPPLE_BASE}/bin/receivepglog receivelog

    ##xscsci
    configXSCSCIMK ${RIPPLE_BASE}/bin/xscsci xscsci

    ##xmanager
    configSpecialMK ${RIPPLE_BASE}/bin/xmanager xmanager

    cd ${RIPPLE_BASE}
}
