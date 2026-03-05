#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "storage/trail/head/ripple_fftrail_head.h"

typedef enum RIPPLE_TRAIL_HEAD_TOKEN
{
    RIPPLE_TRAIL_HEAD_TOKEN_VERSION         = 0x00,
    RIPPLE_TRAIL_HEAD_TOKEN_DBTYPE          = 0x01,
    RIPPLE_TRAIL_HEAD_TOKEN_DBVERSION       = 0x02,
    RIPPLE_TRAIL_HEAD_TOKEN_REDOLSN         = 0x03,
    RIPPLE_TRAIL_HEAD_TOKEN_RESTARTLSN      = 0x04,
    RIPPLE_TRAIL_HEAD_TOKEN_CONFIRMLSN      = 0x05,
    RIPPLE_TRAIL_HEAD_TOKEN_FILENAME        = 0x06,
    RIPPLE_TRAIL_HEAD_TOKEN_FILESIZE        = 0x07,
    RIPPLE_TRAIL_HEAD_TOKEN_COMPATIBILITY   = 0x08,
    RIPPLE_TRAIL_HEAD_TOKEN_ENCRYPTION      = 0x09,
    RIPPLE_TRAIL_HEAD_TOKEN_STARTXID        = 0x0A,
    RIPPLE_TRAIL_HEAD_TOKEN_ENDXID          = 0x0B,
    RIPPLE_TRAIL_HEAD_TOKEN_MAGIC           = 0xFF                  /* 永远在最后 */
} ripple_trail_head_token;

/* 序列化头信息 */
bool ripple_fftrail_head_serail(void* data, void* state)
{
    uint32 len = 0;
    uint8* uptr = NULL;
    ripple_ff_header* ffheader = NULL;
    ripple_file_buffer* rfbuffer = NULL;
    ripple_ffsmgr_state* ffstate = NULL;

    ffheader = (ripple_ff_header*)data;
    ffstate = (ripple_ffsmgr_state*)state;

    /* 获取 buffer */
    rfbuffer = ripple_file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);

    /* 向 buffer 中组装内容 */
    uptr = rfbuffer->data + rfbuffer->start;

    /* 添加文件头信息*/
    /* group info，最后添加 */
    len = RIPPLE_TOKENHDRSIZE;
    uptr += RIPPLE_TOKENHDRSIZE;

    /* ffheader 内容填充 */
    /* 增加 version */
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_HEAD_TOKEN_VERSION,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                RIPPLE_FTRAIL_TOKENDATATYPE_STR,
                                (uint16)strlen(ffheader->version),
                                (uint8*)ffheader->version,
                                &len,
                                uptr);

    /* 增加 dbtype */
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_HEAD_TOKEN_DBTYPE,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                RIPPLE_FTRAIL_TOKENDATATYPE_INT,
                                sizeof(ffheader->dbtype),
                                (uint8*)&ffheader->dbtype,
                                &len,
                                uptr);

    /* 增加 dbversion */
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_HEAD_TOKEN_DBVERSION,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                RIPPLE_FTRAIL_TOKENDATATYPE_STR,
                                strlen(ffheader->dbversion),
                                (uint8*)ffheader->dbversion,
                                &len,
                                uptr);

    /* 增加 redolsn */
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_HEAD_TOKEN_REDOLSN,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                RIPPLE_FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->redolsn),
                                (uint8*)&ffheader->redolsn,
                                &len,
                                uptr);

    /* 增加 restartlsn */
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_HEAD_TOKEN_RESTARTLSN,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                RIPPLE_FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->restartlsn),
                                (uint8*)&ffheader->restartlsn,
                                &len,
                                uptr);

    /* 增加 confirmlsn */
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_HEAD_TOKEN_CONFIRMLSN,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                RIPPLE_FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->confirmlsn),
                                (uint8*)&ffheader->confirmlsn,
                                &len,
                                uptr);

    /* 增加 filename */
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_HEAD_TOKEN_FILENAME,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                RIPPLE_FTRAIL_TOKENDATATYPE_STR,
                                strlen(ffheader->filename),
                                (uint8*)ffheader->filename,
                                &len,
                                uptr);

    /* 增加 filesize */
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_HEAD_TOKEN_FILESIZE,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                RIPPLE_FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->filesize),
                                (uint8*)&ffheader->filesize,
                                &len,
                                uptr);

    /* 增加 compatibility */
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_HEAD_TOKEN_COMPATIBILITY,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                RIPPLE_FTRAIL_TOKENDATATYPE_INT,
                                sizeof(ffheader->compatibility),
                                (uint8*)&ffheader->compatibility,
                                &len,
                                uptr);

    /* 增加 encryption */
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_HEAD_TOKEN_ENCRYPTION,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                RIPPLE_FTRAIL_TOKENDATATYPE_INT,
                                sizeof(ffheader->encryption),
                                (uint8*)&ffheader->encryption,
                                &len,
                                uptr);

    /* 增加 startxid */
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_HEAD_TOKEN_STARTXID,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                RIPPLE_FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->startxid),
                                (uint8*)&ffheader->startxid,
                                &len,
                                uptr);

    /* 增加 endxid */
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_HEAD_TOKEN_ENDXID,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                RIPPLE_FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->endxid),
                                (uint8*)&ffheader->endxid,
                                &len,
                                uptr);

    /* 增加 magic 标识 */
    ffheader->magic = RIPPLE_FTRAIL_MAGIC;
    uptr = ripple_fftrail_token2buffer(RIPPLE_TRAIL_HEAD_TOKEN_MAGIC,
                                RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                RIPPLE_FTRAIL_TOKENDATATYPE_INT,
                                4,
                                (uint8*)&ffheader->magic,
                                &len,
                                uptr);

    /* 字节对齐 */
    uptr = rfbuffer->data + rfbuffer->start;
    RIPPLE_FTRAIL_GROUP2BUFFER(put,
                                RIPPLE_FFTRAIL_GROUPTYPE_FHEADER,
                                RIPPLE_FFTRAIL_INFOTYPE_GROUP,
                                len,
                                uptr)

    /* 此处不偏移，在获取位置信息后偏移 */
    len = RIPPLE_MAXALIGN(len);
    rfbuffer->start += len;
    return true;
}

/* 反序列化信息 */
bool ripple_fftrail_head_deserail(void** data, void* state)
{
    uint8   tokenid = 0;                        /* token 标识 */
    uint8   tokeninfo = 0;                      /* token 的详情 */
    uint32  tokenlen = 0;                       /* token 长度 */
    uint8*  tokendata = NULL;                   /* token 数据区 */
    uint8*  uptr = NULL;
    ripple_ff_header* ffheader = NULL;
    ripple_ffsmgr_state* ffstate = NULL;

    /* 获取 buffer */
    ffstate = (ripple_ffsmgr_state*)state;

    /* 在 buffer 中组装内容 */
    uptr = ffstate->recptr;

    /* 申请空间 */
    ffheader = (ripple_ff_header*)rmalloc0(sizeof(ripple_ff_header));
    if(NULL == ffheader)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader, 0, '\0', sizeof(ripple_ff_header));
    *data = ffheader;

    /* 获取头部标识 */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(RIPPLE_FFTRAIL_GROUPTYPE_FHEADER != tokenid
        || RIPPLE_FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        elog(RLOG_ERROR, "trail file format error");
    }

    /* 重设位置 */
    uptr = ffstate->recptr;
    uptr += RIPPLE_TOKENHDRSIZE;

    /* 获取 version */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->version = (char*)rmalloc0(tokenlen);
    if(NULL == ffheader->version)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader->version, 0, '\0', tokenlen);
    rmemcpy0(ffheader->version, 0, tokendata, tokenlen - RIPPLE_TOKENHDRSIZE);

    /* 获取 dbtype */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->dbtype = RIPPLE_CONCAT(get,32bit)(&tokendata);

    /* 获取 dbversion */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->dbversion = (char*)rmalloc0(tokenlen);
    if(NULL == ffheader->dbversion)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader->dbversion, 0, '\0', tokenlen);
    rmemcpy0(ffheader->dbversion, 0, tokendata, tokenlen - RIPPLE_TOKENHDRSIZE);

    /* 获取 redolsn */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->redolsn = RIPPLE_CONCAT(get,64bit)(&tokendata);

    /* 获取 restartlsn */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->restartlsn = RIPPLE_CONCAT(get,64bit)(&tokendata);

    /* 获取 confirmlsn */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->confirmlsn = RIPPLE_CONCAT(get,64bit)(&tokendata);

    /* 获取 filename */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->filename = (char*)rmalloc0(tokenlen);
    if(NULL == ffheader->filename)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader->filename, 0, '\0', tokenlen);
    rmemcpy0(ffheader->filename, 0, tokendata, tokenlen - RIPPLE_TOKENHDRSIZE);

    /* 获取 filesize */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->filesize = RIPPLE_CONCAT(get,64bit)(&tokendata);

    /* 获取 文件适配版本 */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->compatibility = RIPPLE_CONCAT(get,32bit)(&tokendata);

    /* 获取 encryption */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->encryption = RIPPLE_CONCAT(get,32bit)(&tokendata);

    /* 获取 startxid */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->startxid = RIPPLE_CONCAT(get,64bit)(&tokendata);

    /* 获取 endxid */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->endxid = RIPPLE_CONCAT(get,64bit)(&tokendata);

    /* 获取魔数 */
    RIPPLE_FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->magic = RIPPLE_CONCAT(get,32bit)(&tokendata);

    return true;
}
