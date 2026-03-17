#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/head/fftrail_head.h"

typedef enum TRAIL_HEAD_TOKEN
{
    TRAIL_HEAD_TOKEN_VERSION         = 0x00,
    TRAIL_HEAD_TOKEN_DBTYPE          = 0x01,
    TRAIL_HEAD_TOKEN_DBVERSION       = 0x02,
    TRAIL_HEAD_TOKEN_REDOLSN         = 0x03,
    TRAIL_HEAD_TOKEN_RESTARTLSN      = 0x04,
    TRAIL_HEAD_TOKEN_CONFIRMLSN      = 0x05,
    TRAIL_HEAD_TOKEN_FILENAME        = 0x06,
    TRAIL_HEAD_TOKEN_FILESIZE        = 0x07,
    TRAIL_HEAD_TOKEN_COMPATIBILITY   = 0x08,
    TRAIL_HEAD_TOKEN_ENCRYPTION      = 0x09,
    TRAIL_HEAD_TOKEN_STARTXID        = 0x0A,
    TRAIL_HEAD_TOKEN_ENDXID          = 0x0B,
    TRAIL_HEAD_TOKEN_MAGIC           = 0xFF                  /* 永远在最后 */
} trail_head_token;

/* 序列化头信息 */
bool fftrail_head_serail(void* data, void* state)
{
    uint32 len = 0;
    uint8* uptr = NULL;
    ff_header* ffheader = NULL;
    file_buffer* rfbuffer = NULL;
    ffsmgr_state* ffstate = NULL;

    ffheader = (ff_header*)data;
    ffstate = (ffsmgr_state*)state;

    /* 获取 buffer */
    rfbuffer = file_buffer_getbybufid(ffstate->callback.getfilebuffer(ffstate->privdata), ffstate->bufid);

    /* 向 buffer 中组装内容 */
    uptr = rfbuffer->data + rfbuffer->start;

    /* 添加文件头信息*/
    /* group info，最后添加 */
    len = TOKENHDRSIZE;
    uptr += TOKENHDRSIZE;

    /* ffheader 内容填充 */
    /* 增加 version */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_VERSION,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_STR,
                                (uint16)strlen(ffheader->version),
                                (uint8*)ffheader->version,
                                &len,
                                uptr);

    /* 增加 dbtype */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_DBTYPE,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_INT,
                                sizeof(ffheader->dbtype),
                                (uint8*)&ffheader->dbtype,
                                &len,
                                uptr);

    /* 增加 dbversion */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_DBVERSION,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_STR,
                                strlen(ffheader->dbversion),
                                (uint8*)ffheader->dbversion,
                                &len,
                                uptr);

    /* 增加 redolsn */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_REDOLSN,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->redolsn),
                                (uint8*)&ffheader->redolsn,
                                &len,
                                uptr);

    /* 增加 restartlsn */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_RESTARTLSN,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->restartlsn),
                                (uint8*)&ffheader->restartlsn,
                                &len,
                                uptr);

    /* 增加 confirmlsn */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_CONFIRMLSN,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->confirmlsn),
                                (uint8*)&ffheader->confirmlsn,
                                &len,
                                uptr);

    /* 增加 filename */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_FILENAME,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_STR,
                                strlen(ffheader->filename),
                                (uint8*)ffheader->filename,
                                &len,
                                uptr);

    /* 增加 filesize */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_FILESIZE,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->filesize),
                                (uint8*)&ffheader->filesize,
                                &len,
                                uptr);

    /* 增加 compatibility */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_COMPATIBILITY,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_INT,
                                sizeof(ffheader->compatibility),
                                (uint8*)&ffheader->compatibility,
                                &len,
                                uptr);

    /* 增加 encryption */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_ENCRYPTION,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_INT,
                                sizeof(ffheader->encryption),
                                (uint8*)&ffheader->encryption,
                                &len,
                                uptr);

    /* 增加 startxid */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_STARTXID,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->startxid),
                                (uint8*)&ffheader->startxid,
                                &len,
                                uptr);

    /* 增加 endxid */
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_ENDXID,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_BIGINT,
                                sizeof(ffheader->endxid),
                                (uint8*)&ffheader->endxid,
                                &len,
                                uptr);

    /* 增加 magic 标识 */
    ffheader->magic = FTRAIL_MAGIC;
    uptr = fftrail_token2buffer(TRAIL_HEAD_TOKEN_MAGIC,
                                FFTRAIL_INFOTYPE_TOKEN,
                                FTRAIL_TOKENDATATYPE_INT,
                                4,
                                (uint8*)&ffheader->magic,
                                &len,
                                uptr);

    /* 字节对齐 */
    uptr = rfbuffer->data + rfbuffer->start;
    FTRAIL_GROUP2BUFFER(put,
                                FFTRAIL_GROUPTYPE_FHEADER,
                                FFTRAIL_INFOTYPE_GROUP,
                                len,
                                uptr)

    /* 此处不偏移，在获取位置信息后偏移 */
    len = MAXALIGN(len);
    rfbuffer->start += len;
    return true;
}

/* 反序列化信息 */
bool fftrail_head_deserail(void** data, void* state)
{
    uint8   tokenid = 0;                        /* token 标识 */
    uint8   tokeninfo = 0;                      /* token 的详情 */
    uint32  tokenlen = 0;                       /* token 长度 */
    uint8*  tokendata = NULL;                   /* token 数据区 */
    uint8*  uptr = NULL;
    ff_header* ffheader = NULL;
    ffsmgr_state* ffstate = NULL;

    /* 获取 buffer */
    ffstate = (ffsmgr_state*)state;

    /* 在 buffer 中组装内容 */
    uptr = ffstate->recptr;

    /* 申请空间 */
    ffheader = (ff_header*)rmalloc0(sizeof(ff_header));
    if(NULL == ffheader)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader, 0, '\0', sizeof(ff_header));
    *data = ffheader;

    /* 获取头部标识 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    if(FFTRAIL_GROUPTYPE_FHEADER != tokenid
        || FFTRAIL_INFOTYPE_GROUP != tokeninfo)
    {
        elog(RLOG_ERROR, "trail file format error");
    }

    /* 重设位置 */
    uptr = ffstate->recptr;
    uptr += TOKENHDRSIZE;

    /* 获取 version */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->version = (char*)rmalloc0(tokenlen);
    if(NULL == ffheader->version)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader->version, 0, '\0', tokenlen);
    rmemcpy0(ffheader->version, 0, tokendata, tokenlen - TOKENHDRSIZE);

    /* 获取 dbtype */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->dbtype = CONCAT(get,32bit)(&tokendata);

    /* 获取 dbversion */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->dbversion = (char*)rmalloc0(tokenlen);
    if(NULL == ffheader->dbversion)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader->dbversion, 0, '\0', tokenlen);
    rmemcpy0(ffheader->dbversion, 0, tokendata, tokenlen - TOKENHDRSIZE);

    /* 获取 redolsn */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->redolsn = CONCAT(get,64bit)(&tokendata);

    /* 获取 restartlsn */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->restartlsn = CONCAT(get,64bit)(&tokendata);

    /* 获取 confirmlsn */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->confirmlsn = CONCAT(get,64bit)(&tokendata);

    /* 获取 filename */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->filename = (char*)rmalloc0(tokenlen);
    if(NULL == ffheader->filename)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader->filename, 0, '\0', tokenlen);
    rmemcpy0(ffheader->filename, 0, tokendata, tokenlen - TOKENHDRSIZE);

    /* 获取 filesize */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->filesize = CONCAT(get,64bit)(&tokendata);

    /* 获取 文件适配版本 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->compatibility = CONCAT(get,32bit)(&tokendata);

    /* 获取 encryption */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->encryption = CONCAT(get,32bit)(&tokendata);

    /* 获取 startxid */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->startxid = CONCAT(get,64bit)(&tokendata);

    /* 获取 endxid */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->endxid = CONCAT(get,64bit)(&tokendata);

    /* 获取魔数 */
    FTRAIL_BUFFER2TOKEN(get, uptr, tokenid, tokeninfo, tokenlen, tokendata)
    ffheader->magic = CONCAT(get,32bit)(&tokendata);

    return true;
}
