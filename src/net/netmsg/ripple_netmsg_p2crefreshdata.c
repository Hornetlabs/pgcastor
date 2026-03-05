#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "port/net/ripple_net.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/string/stringinfo.h"
#include "net/netmsg/ripple_netmsg.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "net/netmsg/ripple_netmsg_p2crefreshdata.h"
#include "net/netiomp/ripple_netiomp.h"
#include "misc/ripple_misc_stat.h"
#include "increment/collector/netclient/ripple_increment_collectornetclient.h"


bool ripple_netmsg_p2crefreshdata(void* privdata, uint8* msg)
{
    uint32 msglen = 0;
    uint64 offset = 0;
    StringInfo partialpath = NULL;

    uint8* uptr = NULL;
    ripple_increment_collectornetclient_state* nodesvrstate = NULL;
    ripple_refresh_tablebase* tablebase = NULL;
    ripple_collectornetclient_refreshsharding* refreshshardingstate = NULL;

    uptr = msg;
    nodesvrstate = (ripple_increment_collectornetclient_state*)privdata;
    refreshshardingstate = (ripple_collectornetclient_refreshsharding*)nodesvrstate->data;
    tablebase = &refreshshardingstate->refreshtablebase;

    /* 获取 msglen */
    uptr += 4;
    msglen = RIPPLE_CONCAT(get, 32bit)(&uptr);
    offset = RIPPLE_CONCAT(get, 64bit)(&uptr);

    /* 获取固定的附加信息 */
    msglen -= RIPPLE_NETMSG_TYPE_P2C_REFRESHDATA_FIXSIZE;
    if (0 == msglen)
    {
        return true;
    }

    while(-1 == fcntl(refreshshardingstate->fd, F_GETFL))
    {

        if(true == g_gotsigterm)
        {
            return true;
        }

        partialpath = makeStringInfo();

        appendStringInfo(partialpath, "%s/%s_%s/%s/%s_%s_%u_%u.%s",
                                        refreshshardingstate->refresh_path,
                                        tablebase->schema,
                                        tablebase->table,
                                        RIPPLE_REFRESH_PARTIAL,
                                        tablebase->schema,
                                        tablebase->table,
                                        tablebase->shards,
                                        tablebase->shardnum,
                                        RIPPLE_REFRESH_PARTIAL);

        refreshshardingstate->fd = BasicOpenFile(partialpath->data,
                                                O_RDWR | RIPPLE_BINARY);
        deleteStringInfo(partialpath);
        if (refreshshardingstate->fd < 0)
        {
            
            elog(RLOG_ERROR, "open file error %s", strerror(errno));
        }
    }

    if (FilePWrite(refreshshardingstate->fd, (char*)uptr, msglen, offset) != msglen)
    {
        FileClose(refreshshardingstate->fd);
        elog(RLOG_ERROR, "could not write to file %s_%s_%u_%u", tablebase->schema,
                                                                tablebase->table,
                                                                tablebase->shards,
                                                                tablebase->shardnum);
    }

    if(0 != FileSync(refreshshardingstate->fd))
    {
        FileClose(refreshshardingstate->fd);
        elog(RLOG_ERROR, "could not fsync file %s_%s_%u_%u", tablebase->schema,
                                                            tablebase->table,
                                                            tablebase->shards,
                                                            tablebase->shardnum);
    }

    return true;
}
