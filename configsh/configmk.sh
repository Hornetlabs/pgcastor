#!/bin/bash
#
# All Copyright (c) 2024-2024, Byte Sync Development Group
# 生成 makefile 文件
#

##生成文件头
function configMKHead
{
    echo "#"                                                                    > $1
    echo "# All Copyright (c) 2024-2024, Byte Sync Development Group"           >> $1
    echo "#"                                                                    >> $1
    echo ""                                                                     >> $1
}

######生成 makefileglobal
function configMKGlobal
{
    if [ ${RIPPLE_OP} == "clear" ]
    then
        rm -f Makefile.global
        return
    fi

    ##生成文件头标识
    configMKHead Makefile.global

    ##生成文件内容
    echo "RIPPLE_BASE=${RIPPLE_BASE}"                                           >> Makefile.global
    echo "RIPPLE_SRCDIR=${RIPPLE_SRC}"                                          >> Makefile.global
    echo "RIPPLE_INTERFACE=${RIPPLE_BASE}/interfaces"                            >> Makefile.global
    echo "RIPPLE_INTERFACELIBDIR=${RIPPLE_INTERFACELIBDIR}"                           >> Makefile.global
    echo "RIPPLE_INTERFACEINCLDIR=${RIPPLE_INTERFACEINCLDIR}"                         >> Makefile.global
    echo "RIPPLE_INTERFACESRCDIR=${RIPPLE_INTERFACESRCDIR}"                           >> Makefile.global
    echo "RIPPLE_PARSER=${RIPPLE_BASE}/parser"                                  >> Makefile.global
    echo "RIPPLE_PARSER_SRC=${RIPPLE_BASE}/parser/src"                          >> Makefile.global
    echo "RIPPLE_PARSER_INCL=${RIPPLE_BASE}/parser/include"                     >> Makefile.global
    echo "RIPPLE_INCLDIR=${RIPPLE_BASE}/incl"                                   >> Makefile.global
    echo "RIPPLE_LIBDIR=${RIPPLE_BASE}/lib"                                     >> Makefile.global
    echo "RIPPLE_DB_INCL=${RIPPLE_DB_INCL}"                                     >> Makefile.global
    echo "RIPPLE_DB_LIBDIR=${RIPPLE_DB_LIBDIR}"                                 >> Makefile.global
    echo "RIPPLE_CURL_INCL=${RIPPLE_CURL_INCL}"                                 >> Makefile.global
    echo "RIPPLE_CURL_LIBDIR=${RIPPLE_CURL_LIBDIR}"                             >> Makefile.global
    echo "RIPPLE_BIN=${RIPPLE_BASE}/bin"                                        >> Makefile.global
    echo "RIPPLE_OS_LIB=${RIPPLE_OS_LIB}"                                       >> Makefile.global
    echo "RIPPLE_DB_LIB=${RIPPLE_DB_LIB}"                                       >> Makefile.global
    echo "RIPPLE_CURL_LIB=${RIPPLE_CURL_LIB}"                                   >> Makefile.global
    echo "RIPPLE_INCL=-I\${RIPPLE_INCLDIR} -I\${RIPPLE_DB_INCL} -I\${RIPPLE_CURL_INCL} -I\${RIPPLE_PARSER_INCL}"                >> Makefile.global
    echo "RIPPLE_INTERFACEINCL=-I\${RIPPLE_INTERFACEINCLDIR}"                   >> Makefile.global
    echo "RIPPLE_RPATH=${RIPPLE_RPATH} "                                        >> Makefile.global
    echo "CC=${RIPPLE_CC}"                                                      >> Makefile.global
    echo "RIPPLE_SHARED=-shared"                                                >> Makefile.global
    echo "RIPPLE_INTERFACELIBNAME=${RIPPLE_INTERFACELIBNAME}"                   >> Makefile.global
    echo "RIPPLE_CFLAGS=${RIPPLE_CFLAGS}"                                       >> Makefile.global
}

########################生成 makefile

##即有文件又有文件夹
function configMKSubDirsAndFiles
{
    local index=0
    local dircnt=0
    local cfilenum=0
    local -a subarry=("$@")
    # 校验数组长度 ${#array[@]} ，若不为0,那么需要写入subdir相关信息， -eq  
    dircnt=${#subarry[@]}

    echo "#"                                                                    > Makefile
    echo "#"                                                                    >> Makefile
    echo "include ${RIPPLE_BASE}/Makefile.global"                               >> Makefile
    echo ""                                                                     >> Makefile
    echo "RIPPLE_SOURCE = \$(wildcard *.c)"                                     >> Makefile
    echo "RIPPLE_OBJS = \$(patsubst %.c, %.o, \$(RIPPLE_SOURCE))"               >> Makefile
    echo ""                                                                     >> Makefile
    for direntry in ${subarry[@]}
    do
        cfilenum=`find "$direntry" -name "*.c" | wc -l`
        if [ $cfilenum != 0 ]
        then
            if [ $index -eq 0 ]
            then
                echo "RIPPLE_SUBDIRS = $direntry \\"                               >> Makefile
            else
                echo "				$direntry \\"                                  >> Makefile
            fi
            let index+=1
        fi
        cfilenum=0
    done

    echo ""                                                                             >> Makefile
    echo ""                                                                             >> Makefile
    echo "all:\${RIPPLE_SUBDIRS} \${RIPPLE_OBJS} ok"                                    >> Makefile
    echo ""                                                                             >> Makefile
    echo "\${RIPPLE_SUBDIRS}:ECHO"                                                      >> Makefile
    echo "	make -C \$@"                                                                >> Makefile
    echo ""                                                                             >> Makefile
    echo "ECHO:"                                                                        >> Makefile
    echo "	echo \${RIPPLE_SUBDIRS}"                                                    >> Makefile
    echo ""                                                                             >> Makefile
    echo "\${RIPPLE_OBJS}:%.o:%.c"                                                      >> Makefile
    echo "	\${CC} \${RIPPLE_CFLAGS} \${RIPPLE_INCL} \${${RIPPLE_TEMPINCL}} -c $< -o \$@"                      >> Makefile
    echo ""                                                                             >> Makefile
    echo "ok:"                                                                          >> Makefile
    echo "	cp \${RIPPLE_OBJS} \${${RIPPLE_TMPLIBDIR}}/."                                     >> Makefile
    echo ""                                                                             >> Makefile
    echo "clean:"                                                                       >> Makefile
    echo "	rm -f \${RIPPLE_OBJS}"                                                      >> Makefile
    echo "	find ./ -name '*.o' -delete"                                                >> Makefile
    echo "	for direntry in \$(RIPPLE_SUBDIRS); do \$(MAKE) -C \$\${direntry} clean;done" >> Makefile
    echo ""                                                                             >> Makefile
}


function configMKSubFiles
{
    ##生成文件头标识
    configMKHead Makefile

    echo "include ${RIPPLE_BASE}/Makefile.global"                                       >> Makefile
    echo "RIPPLE_SOURCE = \$(wildcard *.c)"                                             >> Makefile
    echo "RIPPLE_OBJS = \$(patsubst %.c, %.o, \$(RIPPLE_SOURCE))"                       >> Makefile
    echo ""                                                                             >> Makefile
    echo ""                                                                             >> Makefile
    echo "all:\${RIPPLE_OBJS} ok"                                                       >> Makefile
    echo ""                                                                             >> Makefile
    echo "\${RIPPLE_OBJS}:%.o:%.c"                                                      >> Makefile
    echo "	\${CC} \${RIPPLE_CFLAGS} \${RIPPLE_INCL} \${${RIPPLE_TEMPINCL}} -c $< -o \$@"                      >> Makefile
    echo ""                                                                             >> Makefile
    echo "ok:"                                                                          >> Makefile
    echo "	cp \${RIPPLE_OBJS} \${${RIPPLE_TMPLIBDIR}}/."                               >> Makefile
    echo ""                                                                             >> Makefile
    echo "clean:"                                                                       >> Makefile
    echo "	rm -f \${RIPPLE_OBJS}"                                                      >> Makefile
    echo "	find ./ -name '*.o' -delete"                                                >> Makefile
    echo ""                                                                             >> Makefile
}

##目录生成Makefile
function configMKSubDirs
{
    local index=0
    local dircnt=0
    local cfilenum=0
    local -a subarry=("$@")
    # 校验数组长度 ${#array[@]} ，若不为0,那么需要写入subdir相关信息， -eq  
    dircnt=${#subarry[@]}

    ##生成文件头标识
    configMKHead Makefile

    echo "include ${RIPPLE_BASE}/Makefile.global"                               >> Makefile
    echo ""                                                                     >> Makefile
    for direntry in ${subarry[@]}
    do
        cfilenum=`find "$direntry" -name "*.c" | wc -l`
        if [ $cfilenum != 0 ]
        then
            if [ $index -eq 0 ]
            then
                echo "RIPPLE_SUBDIRS = $direntry \\"                               >> Makefile
            else
                echo "				$direntry \\"                                  >> Makefile
            fi
            let index+=1
        fi
        cfilenum=0
    done

    echo ""                                                                     >> Makefile
    echo ""                                                                     >> Makefile
    echo "all:\${RIPPLE_SUBDIRS}"                                               >> Makefile
    echo ""                                                                     >> Makefile
    echo "\${RIPPLE_SUBDIRS}:ECHO"                                              >> Makefile
    echo "	make -C \$@"                                                        >> Makefile
    echo ""                                                                     >> Makefile
    echo "ECHO:"                                                                >> Makefile
    echo "	echo \${RIPPLE_SUBDIRS}"                                            >> Makefile
    echo ""                                                                     >> Makefile
    echo "clean:"                                                               >> Makefile
    echo "	find ./ -name '*.o' -delete"                                        >> Makefile
    echo "	for direntry in \$(RIPPLE_SUBDIRS); do \$(MAKE) -C \$\${direntry} clean;done" >> Makefile
    echo ""                                                                     >> Makefile
}


##目录生成Makefile
function configMKSubDirsNoCheck
{
    local index=0
    local dircnt=0
    local -a subarry=("$@")
    # 校验数组长度 ${#array[@]} ，若不为0,那么需要写入subdir相关信息， -eq  
    dircnt=${#subarry[@]}

    ##生成文件头标识
    configMKHead Makefile

    echo "include ${RIPPLE_BASE}/Makefile.global"                               >> Makefile
    echo ""                                                                     >> Makefile
    for direntry in ${subarry[@]}
    do
        if [ $index -eq 0 ]
        then
            echo "RIPPLE_SUBDIRS = $direntry \\"                               >> Makefile
        else
            echo "				$direntry \\"                                  >> Makefile
        fi
        let index+=1
    done

    echo ""                                                                     >> Makefile
    echo ""                                                                     >> Makefile
    echo "all:\${RIPPLE_SUBDIRS}"                                               >> Makefile
    echo ""                                                                     >> Makefile
    echo "\${RIPPLE_SUBDIRS}:ECHO"                                              >> Makefile
    echo "	make -C \$@"                                                        >> Makefile
    echo ""                                                                     >> Makefile
    echo "ECHO:"                                                                >> Makefile
    echo "	echo \${RIPPLE_SUBDIRS}"                                            >> Makefile
    echo ""                                                                     >> Makefile
    echo "clean:"                                                               >> Makefile
    echo "	find ./ -name '*.o' -delete"                                        >> Makefile
    echo "	for direntry in \$(RIPPLE_SUBDIRS); do \$(MAKE) -C \$\${direntry} clean;done" >> Makefile
    echo ""                                                                     >> Makefile
}


##目录内只有文件生成Makefile
#支持两个入参
# 1、当前路径
# 2、处理路径
function configMKFiles
{
    local subdircnt=0
    local subfilecnt=0
    local getfilec=0
    local genmakefile=0
    local -a subdirs

    cd $2
    for direntry in `ls $2`
    do
        local subfile
        subfile=$2"/"$direntry

        if [ -d $subfile ]
        then
            subdirs[$subdircnt]=$subfile
            let subdircnt+=1
            configMKFiles $2 $subfile
            getfilec=$?
        else
            if [ "c" == "${direntry##*.}" ]
            then
                let subfilecnt+=1
            else
                if [ ${RIPPLE_OP} == "clear" ] && [ "Makefile" == $direntry ]
                then
                    rm -f $subfile
                fi
            fi
        fi
    done

    if [ ${RIPPLE_OP} == "clear" ]
    then
        return
    fi

    if [ $getfilec != 0 ]
    then
        if [ $subfilecnt -gt 0 ] && [ $subdircnt -gt 0 ]
        then
            configMKSubDirsAndFiles "${subdirs[@]}"
            genmakefile=1
        elif [ $subdircnt -gt 0 ]
        then
            configMKSubDirs "${subdirs[@]}"
            genmakefile=1
        fi
    elif [ $subfilecnt -gt 0 ]
    then
        configMKSubFiles
        genmakefile=1
    fi
    cd $1
    return $genmakefile
}


##生成so动态库
function configInterfaceLib
{
    cd ${RIPPLE_INTERFACE}

    ## interfaces 最上层目录
    local subdircnt=0
    local -a subdirs

    subdirs[$subdircnt]=${RIPPLE_BASE}/interfaces/src
    let subdircnt+=1

    subdirs[$subdircnt]=${RIPPLE_BASE}/interfaces/lib
    let subdircnt+=1

    configMKSubDirsNoCheck "${subdirs[@]}"

    cd ${RIPPLE_INTERFACELIBDIR}

    ##生成文件头标识
    configMKHead Makefile

    echo "include ${RIPPLE_BASE}/Makefile.global"                                       >> Makefile
    echo "LIBOBJS = \$(wildcard xsynch_*.o)"                                            >> Makefile
    echo "RIPPLE_SHARELIB = \${RIPPLE_SHARED} "                                         >> Makefile
    echo "RIPPLE_INTERFACELIBFULLNAME=${RIPPLE_INTERFACELIBFULLNAME}"                   >> Makefile
    echo ""                                                                             >> Makefile
    echo "BACKENDOBJS = ripple_mem.o \\
                        ripple_thread.o \\
                        elog.o \\
                        list_func.o \\
                        fd.o \\
                        globals.o \\
                        guc.o  \\
                        ripple_path.o" >> Makefile
    echo ""                                                                                 >> Makefile
    echo ""                                                                                 >> Makefile
    echo "all:backend xsynchso"                                                             >> Makefile
    echo ""                                                                                 >> Makefile
    echo "backend:"                                                                         >> Makefile
    echo "	rm -f ${RIPPLE_INTERFACELIBDIR}/ripple_path.o"                                  >> Makefile
    echo "	cp ${RIPPLE_LIBDIR}/ripple_path.o   ${RIPPLE_INTERFACELIBDIR}/."                >> Makefile
    echo "	rm -f ${RIPPLE_INTERFACELIBDIR}/guc.o"                                          >> Makefile
    echo "	cp ${RIPPLE_LIBDIR}/guc.o           ${RIPPLE_INTERFACELIBDIR}/."                >> Makefile
    echo "	rm -f ${RIPPLE_INTERFACELIBDIR}/globals.o"                                      >> Makefile
    echo "	cp ${RIPPLE_LIBDIR}/globals.o       ${RIPPLE_INTERFACELIBDIR}/."                >> Makefile
    echo "	rm -f ${RIPPLE_INTERFACELIBDIR}/fd.o"                                           >> Makefile
    echo "	cp ${RIPPLE_LIBDIR}/fd.o            ${RIPPLE_INTERFACELIBDIR}/."                >> Makefile
    echo "	rm -f ${RIPPLE_INTERFACELIBDIR}/ripple_mem.o"                                   >> Makefile
    echo "	cp ${RIPPLE_LIBDIR}/ripple_mem.o    ${RIPPLE_INTERFACELIBDIR}/."                >> Makefile
    echo "	rm -f ${RIPPLE_INTERFACELIBDIR}/ripple_thread.o"                                >> Makefile
    echo "	cp ${RIPPLE_LIBDIR}/ripple_thread.o ${RIPPLE_INTERFACELIBDIR}/."                >> Makefile
    echo "	rm -f ${RIPPLE_INTERFACELIBDIR}/elog.o"                                         >> Makefile
    echo "	cp ${RIPPLE_LIBDIR}/elog.o          ${RIPPLE_INTERFACELIBDIR}/."                >> Makefile
    echo "	rm -f ${RIPPLE_INTERFACELIBDIR}/list_func.o"                                    >> Makefile
    echo "	cp ${RIPPLE_LIBDIR}/list_func.o     ${RIPPLE_INTERFACELIBDIR}/."                >> Makefile
    echo ""                                                                                 >> Makefile
    echo "xsynchso:\${backend}"                                                             >> Makefile
    echo "	\${CC} \${RIPPLE_CFLAGS} \${RIPPLE_SHARELIB} -o \${RIPPLE_INTERFACELIBFULLNAME} \${LIBOBJS} \${BACKENDOBJS} -lm -lpthread " >> Makefile
    echo ""                                                                                 >> Makefile
    echo "clean:"                                                                           >> Makefile
    echo "	rm -f *.o"                                                                      >> Makefile
    echo "	rm -f \${RIPPLE_INTERFACELIBFULLNAME}"                                          >> Makefile
}

