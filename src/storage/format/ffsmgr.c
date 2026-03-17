#include "app_incl.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "misc/misc_control.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"

/* 格式化入口 */
typedef struct FFORMATSMGR
{
    ffsmgr_if_type   type;
    ffsmgr_if*       ffsmgr;
} fformatsmgr;

/* trail 格式化 */
static ffsmgr_if m_ffsmgrtrail =
{
    .ffsmgr_init = fftrail_init,
    .ffsmgr_serial = fftrail_serial,
    .ffsmgr_deserial = fftrail_deserial,
    .ffsmgr_free = fftrail_free,
    .ffsmg_gettokenminsize = fftrail_gettokenminsize,
    .ffsmg_gettailsize = fftrail_taillen,
    .ffsmgr_validrecord = fftrail_validrecord,
    .ffsmgr_getrecordsubtype = fftrail_getrecordsubtype,
    .ffsmgr_getrecordlsn = fftrail_getrecordlsn,
    .ffsmgr_getrecordgrouptype = fftrail_getrecordgrouptype,
    .ffsmgr_getrecorddataoffset = fftrail_getrecorddataoffset,
    .ffsmgr_getrecordtotallength = fftrail_getrecordtotallength,
    .ffsmgr_getrecordlength = fftrail_getrecordlength,
    .ffsmgr_setrecordlength = fftrail_setrecordlength,
    .ffsmgr_isrecordtransstart = fftrail_isrecordtransstart
};

static fformatsmgr m_ffsmgrsw[] =
{
    {FFSMG_IF_TYPE_TRAIL, &m_ffsmgrtrail}
};

/* 初始化，设置使用的格式化接口 */
void ffsmgr_init(ffsmgr_if_type fftype, ffsmgr_state* ffsmgrstate)
{
    ffsmgrstate->ffsmgr = m_ffsmgrsw[fftype].ffsmgr;
}

/* 初始化文件头部信息 */
void* ffsmgr_headinit(int compatibility, FullTransactionId xid, uint64 fileid)
{
    ff_header* ffheader = NULL;

    ffheader = (ff_header*)rmalloc0(sizeof(ff_header));
    if(NULL == ffheader)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader, 0, '\0', sizeof(ff_header));
    ffheader->magic = MAGIC;
    ffheader->compatibility = compatibility;
    ffheader->dbtype = g_idbtype;
    ffheader->dbversion = (char*)guc_getConfigOption(CFG_KEY_DBVERION);
    ffheader->encryption = 0;
    ffheader->endxid = InvalidFullTransactionId;

    /* 拼装路径 */
    ffheader->filename = (char*)rmalloc0(MAXPATH);
    if(NULL == ffheader->filename)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader->filename, 0, '\0', MAXPATH);
    snprintf(ffheader->filename, MAXPATH, "%s/%016lX", STORAGE_TRAIL_DIR, fileid);

    ffheader->filesize = guc_getConfigOptionInt(CFG_KEY_TRAIL_MAX_SIZE);
    ffheader->filesize = (ffheader->filesize * 1024 * 1024);
    ffheader->redolsn = 0;
    ffheader->restartlsn = 0;
    ffheader->startxid = xid;

    /* 版本信息 */
    ffheader->version = VERSION_STR;

    return ffheader;
}

/* 初始化数据库信息 */
void* ffsmgr_dbmetadatainit(char* dbname)
{
    ff_dbmetadata* ffdbmd = NULL;

    ffdbmd = (ff_dbmetadata*)rmalloc0(sizeof(ff_dbmetadata));
    if(NULL == ffdbmd)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffdbmd, 0, '\0', sizeof(ff_dbmetadata));

    /* 填充值信息 */
    ffdbmd->header.dbmdno = 0;
    ffdbmd->header.tbmdno = 0;
    ffdbmd->header.type = FF_DATA_TYPE_DBMETADATA;
    ffdbmd->header.transid = InvalidFullTransactionId;
    ffdbmd->header.transind = 0;
    ffdbmd->header.totallength = 0;
    ffdbmd->header.reclength = 0;
    ffdbmd->header.reccount = 1;
    ffdbmd->header.formattype = FF_DATA_FORMATTYPE_SQL;
    ffdbmd->header.subtype = FF_DATA_TYPE_DBMETADATA;

    ffdbmd->dbname = dbname;
    ffdbmd->money = MONETARY;
    ffdbmd->timezone = TIMEZONE;
    ffdbmd->charset = ORGENCODING;
    return ffdbmd;
}
