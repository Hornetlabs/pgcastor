#!/bin/bash
#
# All Copyright (c) 2024-2024, Byte Sync Development Group
#

#
#---- FOR MODIFY MAKE.sh
#---- $# 函数入参的个数
#---- $@ 函数入参以数组的形式获取
#

source configsh/configvar.sh

source configsh/configmk.sh

source configsh/configparser.sh

source configsh/configbin.sh

source configsh/configtop.sh

source configsh/configdefault.sh

function makeUsage
{
    echo "please use make.sh like:./make.sh [debug=on/off] [dbpath=/home/user/pg/install] [op=/clear/init] "
    exit
}

##参数验证
checkArgv $#

##参数解析
parserArgv $@

##参数验证
validateArgv

##初始化
initVar

##生成 makefile
configMKGlobal

##src目录生成Makefile
RIPPLE_TEMPINCL="RIPPLE_INCL"
RIPPLE_TMPLIBDIR="RIPPLE_LIBDIR"
configMKFiles ${RIPPLE_BASE} ${RIPPLE_SRC}

##parser目录生成Makefile
RIPPLE_TMPLIBDIR="RIPPLE_LIBDIR"
configMKFiles ${RIPPLE_BASE} ${RIPPLE_PARSER_SRC}

##interface 目录下生成 Makefile
RIPPLE_TEMPINCL="RIPPLE_INTERFACEINCL"
RIPPLE_TMPLIBDIR="RIPPLE_INTERFACELIBDIR"
configMKFiles ${RIPPLE_BASE} ${RIPPLE_INTERFACESRCDIR}

mkdir -p ${RIPPLE_LIBDIR}
mkdir -p ${RIPPLE_INTERFACELIBDIR}

##生成 bin 中的内容
configMKBin 

##生成 top 目录
configTop

##生成 parser top 目录
configParserTop

## 生成 config 文件
configInterfaceLib

## 生成版本头文件和默认行为
configdefault

echo "---------------------------------------"
echo "|   config makefile success  |"
echo "---------------------------------------"
