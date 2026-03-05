#!/bin/bash
#
# All Copyright (c) 2024-2024, Byte Sync Development Group
#.通用信息
#---------------------------------------------------------

##基础目录
declare RIPPLE_BASE

##代码主目录
declare RIPPLE_SRC

##interface 目录
declare RIPPLE_INTERFACE
declare RIPPLE_INTERFACESRCDIR

declare RIPPLE_INTERFACEINCLDIR

declare RIPPLE_INTERFACELIBDIR

declare RIPPLE_INTERFACELIBNAME
declare RIPPLE_INTERFACELIBFULLNAME

##库目录
declare RIPPLE_LIB

##头文件主路径
declare RIPPLE_INCL

##数据库安装目录
declare RIPPLE_DB

##动态库/静态库 放置的位置
declare RIPPLE_LIBDIR

##使用的编译器
declare RIPPLE_CC

##数据库动态库目录
declare RIPPLE_DB_LIBDIR

##数据库头文件目录
declare RIPPLE_DB_INCL

##数据库相关的 动态库 名称
declare RIPPLE_DB_LIB

##CURL安装目录
declare RIPPLE_CURL

##CURL动态库目录
declare RIPPLE_CURL_LIBDIR

##CURL头文件目录
declare RIPPLE_CURL_INCL

##CURL相关的 动态库 名称
declare RIPPLE_CURL_LIB

##系统相关的 动态库 名称
declare RIPPLE_OS_LIB

##设置默认的 rpath 路径
declare RIPPLE_RPATH

##开启 debug 标识
declare RIPPLE_DEBUG

##开启 memcheck 的标识
declare RIPPLE_MEMCHECK

##编译选项
declare RIPPLE_CFLAGS="-D_GNU_SOURCE -fPIC -Wall -Wunused-but-set-variable -D __STDC_FORMAT_MACROS -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement"

##操作类型
declare RIPPLE_OP="init"


## 暂时的动态库目录
declare RIPPLE_TMPLIBDIR
declare RIPPLE_TEMPINCL

##设置 debug 值
function setDebug
{
    if [ $1 == "on" ]
    then
        RIPPLE_DEBUG="-g"
    else
        RIPPLE_DEBUG=""
    fi
}

##设置 memcheck
function setMemcheck
{
    if [ $1 == "on" ]
    then
        RIPPLE_MEMCHECK=" -D RIPPLE_MEMCHECK"
    else
        RIPPLE_MEMCHECK=""
    fi
}

##设置操作类型
function setOp
{
    if [ $1 == "clear" ]
    then
        RIPPLE_OP="clear"
    elif [ $1 == "init" ]
    then
        RIPPLE_OP="init"
    else
        echo "unknown op $1 !!!"
        exit
    fi
}

##入参检测
function checkArgv
{
    if [ "$1" -le 0 ]
    then
        makeUsage
    fi
}

##解析入参
function parserArgv
{
    local index=0
    local dircnt=0
    local -a argvs=("$@")

    dircnt=${#argvs[@]}
    for argvitem in ${argvs[@]}
    do
        local oldifs="$IFS"
        IFS="="
        local -a pairs=($argvitem)
        IFS="$oldifs"

        if [ "${pairs[0]}" == "" ]
        then
            makeUsage
        fi

        if [ "${pairs[1]}" == "" ]
        then
            echo "${pairs[0]} value can't be null"
            exit
        fi

        if [ "${pairs[0]}" == "debug" ]
        then
            setDebug ${pairs[1]}
        elif [ "${pairs[0]}" == "dbpath" ]
        then
            RIPPLE_DB=${pairs[1]}
            elif [ "${pairs[0]}" == "curlpath" ]
        then
            RIPPLE_CURL=${pairs[1]}
        elif [ "${pairs[0]}" == "memcheck" ]
        then
            setMemcheck ${pairs[1]}
        elif [ "${pairs[0]}" == "op" ]
        then
            setOp ${pairs[1]}
        else
            echo "unknown argv ${pairs[0]}"
            makeUsage
        fi
    done
}

##参数校验
function validateArgv
{
    if [ "${RIPPLE_DB}" == "" ]
    then
        echo "need set argv dbpath=DB_INSTALL_PATH"
        exit
    fi

    if [ "${RIPPLE_CURL}" == "" ]
    then
        echo "need set argv curlpath=CURL_INSTALL_PATH"
        exit
    fi
}

##初始化变量信息
function initVar
{
    RIPPLE_BASE=`pwd`
    RIPPLE_SRC=${RIPPLE_BASE}/src
    RIPPLE_INCL=${RIPPLE_BASE}/incl
    RIPPLE_LIBDIR=${RIPPLE_BASE}/lib
    RIPPLE_INTERFACE=${RIPPLE_BASE}/interfaces
    RIPPLE_INTERFACESRCDIR=${RIPPLE_INTERFACE}/src
    RIPPLE_INTERFACEINCLDIR=${RIPPLE_INTERFACE}/incl
    RIPPLE_INTERFACELIBDIR=${RIPPLE_INTERFACE}/lib
    RIPPLE_INTERFACELIBFULLNAME="libxsynch.so"
    RIPPLE_INTERFACELIBNAME="-lxsynch"
    RIPPLE_DB_LIBDIR=${RIPPLE_DB}/lib
    RIPPLE_DB_INCL=${RIPPLE_DB}/include
    RIPPLE_CURL_LIBDIR=${RIPPLE_CURL}/lib
    RIPPLE_CURL_INCL=${RIPPLE_CURL}/include
    RIPPLE_CC=gcc
    RIPPLE_DB_LIB="-lpq"
    RIPPLE_CURL_LIB="-lcurl"
    RIPPLE_OS_LIB="-lpthread -lm -llz4"
    RIPPLE_PARSER_BASE=${RIPPLE_BASE}/parser
    RIPPLE_PARSER_SRC=${RIPPLE_BASE}/parser/src
    RIPPLE_RPATH="-Wl,-rpath,${RIPPLE_BASE}/lib"

    ##组装参数内容
    RIPPLE_CFLAGS="${RIPPLE_CFLAGS} ${RIPPLE_MEMCHECK} ${RIPPLE_DEBUG}"
}
