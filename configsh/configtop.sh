#!/bin/bash
#
# All Copyright (c) 2024-2024, Byte Sync Development Group
#.生成 top 下的内容
#---------------------------------------------------------

function configTop
{
    if [ ${RIPPLE_OP} == "clear" ]
    then
        rm -f ${RIPPLE_BASE}/Makefile
        return
    fi

    cd ${RIPPLE_BASE}
    local subdircnt=0
    local -a subdirs

    subdirs[$subdircnt]=${RIPPLE_BASE}/parser
    let subdircnt+=1

    subdirs[$subdircnt]=${RIPPLE_BASE}/src
    let subdircnt+=1

    subdirs[$subdircnt]=${RIPPLE_BASE}/interfaces
    let subdircnt+=1

    subdirs[$subdircnt]=${RIPPLE_BASE}/bin
    let subdircnt+=1

    configMKSubDirs "${subdirs[@]}"
}

function configParserTop
{
    if [ ${RIPPLE_OP} == "clear" ]
    then
        rm -f ${RIPPLE_PARSER_BASE}/Makefile
        return
    fi

    cd ${RIPPLE_PARSER_BASE}
    local subdircnt=0
    local -a subdirs

    subdirs[$subdircnt]=${RIPPLE_PARSER_BASE}/src
    let subdircnt+=1

    configMKSubDirs "${subdirs[@]}"
}
