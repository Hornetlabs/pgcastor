#!/bin/bash
#
# All Copyright (c) 2024-2024, Byte Sync Development Group
#.生成 配置文件 下的内容
#---------------------------------------------------------

function configdefault
{
    local cfgfile=${RIPPLE_INCL}/ripple_config.h
    local platform=`uname -p`
    local cdate=`date`
    local xbuildinfo="XSynch build at ${cdate} on ${platform} platform."

    echo "#ifndef _RIPPLE_CONFIG_H_"                                                            > ${cfgfile}
    echo "#define _RIPPLE_CONFIG_H_"                                                            >> ${cfgfile}

    echo "/* generate by make */"                                                               >> ${cfgfile}

    echo ""                                                                                     >> ${cfgfile}
    echo "#define RMANAGER_UNIXDOMAINDIR         \"/tmp\""                                      >> ${cfgfile}
    echo "#define RMANAGER_UNIXDOMAINPREFIX      \".s.XSYNCH.\""                                >> ${cfgfile}
    echo "#define RMANAGER_PORT                  \"6543\""                                      >> ${cfgfile}
    echo "#define XSYNCH_BUILD                   \"${xbuildinfo}\""                             >> ${cfgfile}
    echo ""                                                                                     >> ${cfgfile}
    echo ""                                                                                     >> ${cfgfile}
    echo "#define RIPPLE_CACHELINE_SIZE           128"                                          >> ${cfgfile}

    echo "#endif"                                                                               >> ${cfgfile}
}



