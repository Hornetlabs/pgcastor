#include "ripple_app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/algorithm/crc/crc_check.h"
#include "queue/ripple_queue.h"
#include "fastcompare/ripple_fastcompare_chunk.h"
#include "fastcompare/ripple_fastcompare_endslice.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netserver.h"
#include "net/ripple_netclient.h"

uint8 *ripple_fastcompare_endslice_serial(uint32 *size)
{
    uint32 len = 0;
    uint32 mlen = 0;
    uint8 *result = NULL;
    uint8 *curptr = NULL;
    uint32 crc = 0;

    /* 计算长度 */
    len = 4 + 4 + 4;    /* type, length, crc32 */

    mlen = RIPPLE_MAXALIGN(len);

    result = rmalloc0(sizeof(uint8) * mlen);
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(uint8) * mlen);

    curptr = result;

    /* type */
    RIPPLE_CONCAT(put, 32bit)(&curptr, RIPPLE_NETMSG_TYPE_FASTCOMPARE_ENDSLICE);
    /* len */
    RIPPLE_CONCAT(put, 32bit)(&curptr, len);

    INIT_CRC32C(crc);
    COMP_CRC32C(crc, result, 8);
    FIN_CRC32C(crc);

    /* crc */
    RIPPLE_CONCAT(put, 32bit)(&curptr, crc);

    *size = len;
    return result;
}

bool ripple_fastcompare_endslice_send(void* netclient_in)
{
    ripple_netclient* netclient = (ripple_netclient*) netclient_in;
    ripple_netpacket *netpacket = NULL;
    uint8 *data = NULL;
    uint32 size = 0;

    /* 序列化数据 */
    data = ripple_fastcompare_endslice_serial(&size);

    netpacket = ripple_netpacket_init();
    netpacket->used = size;
    netpacket->max = RIPPLE_MAXALIGN(size);
    netpacket->data = data;
    ripple_netclient_addwpacket(netclient, (void*)netpacket);

    /* 消息处理 */
    if(false == ripple_netclient_desc(netclient))
    {
        netclient->status = RIPPLE_NETCLIENTCONN_STATUS_NOP;

        elog(RLOG_ERROR, "can't send end slice");
    }
    return true;
}
