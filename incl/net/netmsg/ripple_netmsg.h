#ifndef _RIPPLE_NETMSG_H
#define _RIPPLE_NETMSG_H



typedef enum RIPPLE_NETMSG_TYPE
{
    /* 保留 */
    RIPPLE_NETMSG_TYPE_NOP                  = 0x00,

    /* pump 到 collector 的心跳包 */
    RIPPLE_NETMSG_TYPE_P2C_HB               = 0x01,

    /* collector 到 pump 的心跳包 */
    RIPPLE_NETMSG_TYPE_C2P_HB               = 0x02,

    /* pump 到 collector 获取解析起始地址 */
    RIPPLE_NETMSG_TYPE_P2C_IDENTITY         = 0x03,

    /* collector 到 pump 返回解析起始地址 */
    RIPPLE_NETMSG_TYPE_C2P_IDENTITY         = 0x04,

    /* pump 到 collector 数据信息 */
    RIPPLE_NETMSG_TYPE_P2C_DATA             = 0x05,

    /* pump 到 collector 开始refresh */
    RIPPLE_NETMSG_TYPE_P2C_BEGINREFRESH     = 0x06,

    /* collector到 pump 开始refresh */
    RIPPLE_NETMSG_TYPE_C2P_BEGINREFRESH     = 0x07,

    /* pump 到 collector refresh数据信息 */
    RIPPLE_NETMSG_TYPE_P2C_REFRESHDATA      = 0x08,

    /* pump 到 collector endrefresh */
    RIPPLE_NETMSG_TYPE_P2C_ENDREFRESH       = 0x09,

    /* collector到 pump endrefresh */
    RIPPLE_NETMSG_TYPE_C2P_ENDREFRESH       = 0x0A,

    RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_INC_DATA ,

    RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_BEGIN ,

    RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_BEGIN ,

    RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_DATA ,

    RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_END ,

    RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_END ,

    RIPPLE_NETMSG_TYPE_P2C_BIGTXN_BEGIN ,

    RIPPLE_NETMSG_TYPE_P2C_BIGTXN_DATA ,

    RIPPLE_NETMSG_TYPE_P2C_BIGTXN_END ,

    RIPPLE_NETMSG_TYPE_C2P_BIGTXN_END ,

    RIPPLE_NETMSG_TYPE_FASTCOMPARE_BEGINSLICE,

    RIPPLE_NETMSG_TYPE_FASTCOMPARE_BEGINCHNUK,

    RIPPLE_NETMSG_TYPE_FASTCOMPARE_SIMPLEDATACHUNK,

    RIPPLE_NETMSG_TYPE_FASTCOMPARE_DATACMPRESULT,

    RIPPLE_NETMSG_TYPE_FASTCOMPARE_ENDCHUNK,

    RIPPLE_NETMSG_TYPE_FASTCOMPARE_S2DCORRECTDATACHUNK,

    RIPPLE_NETMSG_TYPE_FASTCOMPARE_D2SCORRECTDATACHUNK,

    RIPPLE_NETMSG_TYPE_FASTCOMPARE_ENDSLICE,

    /* 消息类型在此之前添加 */
    RIPPLE_NETMSG_TYPE_MAX

} ripple_netmsg_type;

/* 网络协议头部固定长度 */
#define RIPPLE_NETMSG_TYPE_HDR_SIZE                     8

/*
 * pump-->collector 获取位置信息包 
 * 4(type) + 4(length)      8
 * type                     1
 * jobnamelen               4
 */
#define RIPPLE_NETMSG_TYPE_P2C_IDENTITY_SIZE            13

/* 4(type) + 4(length) + 1(type) + 8(pfileid) + 8 (cfileid) */
/* collector-->pump addr size */
#define RIPPLE_NETMSG_TYPE_C2P_IDENTITY_SIZE            25

/* 4(type) + 4(length) */
/* pump-->collector heartbeat(心跳) 包 */
#define RIPPLE_NETMSG_TYPE_P2C_HB_SIZE                  8

/* 4(type) + 4(length) + 1(type) + 8(fileid) + 8 (cfileid) */
/* collector-->pump heartbeat(心跳) 包 */
#define RIPPLE_NETMSG_TYPE_C2P_HB_SIZE                  25

/*
 * 4(type) + 4(length)      8
 * schlen                   2
 * tblen                    2
 * 分片个数                  4
 * 分片编号                  4
 */
#define RIPPLE_NETMSG_TYPE_P2C_BEGINREFRESH_SIZE        20

/* 4(type) + 4(length)*/
/* collector-->pump beginrefresh size */
#define RIPPLE_NETMSG_TYPE_C2P_BEGINREFRESH_SIZE        8

/* 4(type) + 4(length)*/
/* collector-->pump endrefresh size */
#define RIPPLE_NETMSG_TYPE_C2P_ENDREFRESH_SIZE          8

/* 4(type) + 4(length)*/
/* collector-->pump endrefresh size */
#define RIPPLE_NETMSG_TYPE_P2C_ENDREFRESH_SIZE          8

/* 4(type) + 4(length)*/
/* collector-->pump end bigtxn size */
#define RIPPLE_NETMSG_TYPE_C2P_ENDBIGTXN_SIZE           8

/*
 * 4(type) + 4(length)      8
 * fileid                   8
 * offset                   8
 */
#define RIPPLE_NETMSG_TYPE_P2C_BIGTXN_DATA_SIZE         24

/*
 * 4(type) + 4(length)      8
 * redolsn                  8
 * restartlsn               8
 * confirmlsn               8
 * pfileid                  8           Pump端读取的 Trail 文件的编号
 * cfileid                  8           Pump 重组后的 Trail 文件的编号
 * cblknum                  8
 * coffset                  8           Pump 重组后的基于 Trail 文件头的偏移
 * timestamp                8           事务提交时间戳
 */
#define RIPPLE_NETMSG_TYPE_P2C_DATA_FIXSIZE             72

/*
 * 4(type) + 4(length)      8
 * xid                      8           事务号
 */
#define RIPPLE_NETMSG_TYPE_P2C_BIGTXN_BEGIN_FIXSIZE     16

/*
 * 4(type) + 4(length)      8
 * fileid                   8           Pump端读取的 Trail 文件的编号
 * offset                   8           Pump端读取的 Trail 文件头的偏移
 */
#define RIPPLE_NETMSG_TYPE_P2C_BIGTXN_DATA_FIXSIZE      24

/*
 * 4(type) + 4(length)      8
 */
#define RIPPLE_NETMSG_TYPE_P2C_BIGTXN_END_FIXSIZE       8

/*
 * 4(type) + 4(length)      8
 * offset                   8           Pump 重组后的基于 Trail 文件头的偏移
 */
#define RIPPLE_NETMSG_TYPE_P2C_REFRESHDATA_FIXSIZE      16

/*-----------------onlinerefresh netmsglen  begin------------------------*/

/* 4(type) + 4(length) + 1(type) + 4(jobnamelen) + 16(uuid) 不包含实际jobname内容 */
/* pump-->collector 获取位置信息包 */
#define RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_IDENTITY_SIZE            29

/*
 * 4(type) + 4(length)      8
 * onlinerefresh编号        16
 * schlen                   2
 * tblen                    2
 * 分片个数                  4
 * 分片编号                  4
 */
#define RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_BEGIN_SIZE        36

/* 4(type) + 4(length)*/
/* collector-->pump beginrefresh size */
#define RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_BEGIN_SIZE        8

/* 4(type) + 4(length)*/
/* collector-->pump endrefresh size */
#define RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_END_SIZE          8

/* 4(type) + 4(length)*/
/* collector-->pump endrefresh size */
#define RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_END_SIZE          8

/*
 * 4(type) + 4(length)      8
 * redolsn                  8
 * restartlsn               8
 * confirmlsn               8
 * pfileid                  8           Pump端读取的 Trail 文件的编号
 * cfileid                  8           Pump 重组后的 Trail 文件的编号
 * cblknum                  8
 * coffset                  8           Pump 重组后的基于 Trail 文件头的偏移
 * exit                     1           pump增量数据退出标记
 */
#define RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_INC_DATA_SIZE              65

/*
 * 4(type) + 4(length)      8
 * offset                   8           Pump 重组后的基于 Trail 文件头的偏移
 */
#define RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_SHARDING_DATA_FIXSIZE      16

/*----------------- onlinerefresh netmsglen  end------------------------*/

/* 消息分发处理 */
bool ripple_netmsg(void* privdata,
                    uint32 msgtype,
                    uint8* msg);

#endif
