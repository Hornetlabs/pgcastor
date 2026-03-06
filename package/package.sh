#!/bin/bash

declare CPUTTYPE=x86
declare DATEINFO=2025_06_30
declare NUBER=01
declare RBASE=/opt/ripple
declare RINSTALL=/opt/ripple/install

function pMakeUsage
{
    echo "please use package.sh like:./package.sh rhome=/opt/ripple number=01"
    exit
}

## 入参检测
function pCheckArgv
{
    if [ "$1" -le 0 ]
    then
        pMakeUsage
    fi
}


## 解析入参
function pParserArgv
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
            pMakeUsage
        fi

        if [ "${pairs[1]}" == "" ]
        then
            echo "${pairs[0]} value can't be null"
            exit
        fi

        if [ "${pairs[0]}" == "rhome" ]
        then
            # 入参校验
            RBASE="${pairs[1]}"
            if [ ! -f ${RBASE}/make.sh ];then
                echo "rhome is not coding catalog."
                exit
            fi
        elif [ "${pairs[0]}" == "number" ]
        then
            NUBER=${pairs[1]}
        else
            echo "unknown argv ${pairs[0]}"
            pMakeUsage
        fi
    done
}

## 获取CPU架构和当前日期
function pGetMachineDate
{
    CPUTTYPE=`uname -m`
    DATEINFO=`date +%Y_%m_%d`
}

## copy 文件
function pCopyFile
{
    # 安装路径,并创建该路径 bin etc
    RINSTALL=ripplev2_${DATEINFO}_${CPUTTYPE}_${NUBER}
    #rm -rf $RINSTALL
    
    mkdir -p  ${RINSTALL}/bin
    mkdir -p  ${RINSTALL}/bin/pgreceivelog
    mkdir -p  ${RINSTALL}/config
    mkdir -p  ${RINSTALL}/lib

    # 将执行文件 copy 到对应的目录中
    cp ${RBASE}/bin/capture/capture                  $RINSTALL/bin/.
    cp ${RBASE}/bin/integrate/integrate              $RINSTALL/bin/.
    cp ${RBASE}/bin/receivepglog/receivelog          $RINSTALL/bin/pgreceivelog/.
    cp ${RBASE}/bin/xmanager/xmanager                $RINSTALL/bin/.
    cp ${RBASE}/bin/xscsci/xscsci                    $RINSTALL/bin/.
    cp ${RBASE}/interfaces/lib/libxsynch.so          $RINSTALL/lib/.

    # 将配置文件复制到 etc 目录
    cp -rf ${RBASE}/etc/sample                       $RINSTALL/config/.

    # 压缩包
    #tar -czvf $RINSTALL.tgz $RINSTALL
}

## 参数验证
pCheckArgv $#

## 参数解析
pParserArgv $@

## 获取 CPU 架构和日期
pGetMachineDate

## 创建包、复制文件、压缩包
pCopyFile

echo "package success"
