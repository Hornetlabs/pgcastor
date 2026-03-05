#include "ripple_app_incl.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "misc/ripple_misc_control.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"

/* 格式化入口 */
typedef struct RIPPLE_FFORMATSMGR
{
    ripple_ffsmgr_if_type   type;
    ripple_ffsmgr_if*       ffsmgr;
} ripple_fformatsmgr;

/* trail 格式化 */
static ripple_ffsmgr_if m_ffsmgrtrail =
{
    .ffsmgr_init = ripple_fftrail_init,
    .ffsmgr_serial = ripple_fftrail_serial,
    .ffsmgr_deserial = ripple_fftrail_deserial,
    .ffsmgr_free = ripple_fftrail_free,
    .ffsmg_gettokenminsize = ripple_fftrail_gettokenminsize,
    .ffsmg_gettailsize = ripple_fftrail_taillen,
    .ffsmgr_validrecord = ripple_fftrail_validrecord,
    .ffsmgr_getrecordsubtype = ripple_fftrail_getrecordsubtype,
    .ffsmgr_getrecordlsn = ripple_fftrail_getrecordlsn,
    .ffsmgr_getrecordgrouptype = ripple_fftrail_getrecordgrouptype,
    .ffsmgr_getrecorddataoffset = ripple_fftrail_getrecorddataoffset,
    .ffsmgr_getrecordtotallength = ripple_fftrail_getrecordtotallength,
    .ffsmgr_getrecordlength = ripple_fftrail_getrecordlength,
    .ffsmgr_setrecordlength = ripple_fftrail_setrecordlength,
    .ffsmgr_isrecordtransstart = ripple_fftrail_isrecordtransstart
};

static ripple_fformatsmgr m_ffsmgrsw[] =
{
    {RIPPLE_FFSMG_IF_TYPE_TRAIL, &m_ffsmgrtrail}
};

/* 初始化，设置使用的格式化接口 */
void ripple_ffsmgr_init(ripple_ffsmgr_if_type fftype, ripple_ffsmgr_state* ffsmgrstate)
{
    ffsmgrstate->ffsmgr = m_ffsmgrsw[fftype].ffsmgr;
}

/* 初始化文件头部信息 */
void* ripple_ffsmgr_headinit(int compatibility, FullTransactionId xid, uint64 fileid)
{
    ripple_ff_header* ffheader = NULL;

    ffheader = (ripple_ff_header*)rmalloc0(sizeof(ripple_ff_header));
    if(NULL == ffheader)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader, 0, '\0', sizeof(ripple_ff_header));
    ffheader->magic = RIPPLE_MAGIC;
    ffheader->compatibility = compatibility;
    ffheader->dbtype = g_idbtype;
    ffheader->dbversion = (char*)guc_getConfigOption(RIPPLE_CFG_KEY_DBVERION);
    if(strlen(ffheader->dbversion) == strlen(RIPPLE_DBVERSION_HIGHGO_V903)
            && 0 == strcmp(ffheader->dbversion, RIPPLE_DBVERSION_HIGHGO_V903))
    {
        ffheader->dbversion = RIPPLE_DBVERSION_HIGHGO_V901;
    }
    ffheader->encryption = 0;
    ffheader->endxid = InvalidFullTransactionId;

    /* 拼装路径 */
    ffheader->filename = (char*)rmalloc0(RIPPLE_MAXPATH);
    if(NULL == ffheader->filename)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffheader->filename, 0, '\0', RIPPLE_MAXPATH);
    snprintf(ffheader->filename, RIPPLE_MAXPATH, "%s/%016lX", RIPPLE_STORAGE_TRAIL_DIR, fileid);

    ffheader->filesize = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE);
    ffheader->filesize = (ffheader->filesize * 1024 * 1024);
    ffheader->redolsn = 0;
    ffheader->restartlsn = 0;
    ffheader->startxid = xid;

    /* 版本信息 */
    ffheader->version = RIPPLE_VERSION_STR;

    return ffheader;
}

/* 初始化数据库信息 */
void* ripple_ffsmgr_dbmetadatainit(char* dbname)
{
    ripple_ff_dbmetadata* ffdbmd = NULL;

    ffdbmd = (ripple_ff_dbmetadata*)rmalloc0(sizeof(ripple_ff_dbmetadata));
    if(NULL == ffdbmd)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ffdbmd, 0, '\0', sizeof(ripple_ff_dbmetadata));

    /* 填充值信息 */
    ffdbmd->header.dbmdno = 0;
    ffdbmd->header.tbmdno = 0;
    ffdbmd->header.type = RIPPLE_FF_DATA_TYPE_DBMETADATA;
    ffdbmd->header.transid = InvalidFullTransactionId;
    ffdbmd->header.transind = 0;
    ffdbmd->header.totallength = 0;
    ffdbmd->header.reclength = 0;
    ffdbmd->header.reccount = 1;
    ffdbmd->header.formattype = RIPPLE_FF_DATA_FORMATTYPE_SQL;
    ffdbmd->header.subtype = RIPPLE_FF_DATA_TYPE_DBMETADATA;

    ffdbmd->dbname = dbname;
    ffdbmd->money = RIPPLE_MONETARY;
    ffdbmd->timezone = RIPPLE_TIMEZONE;
    ffdbmd->charset = RIPPLE_ORGENCODING;
    return ffdbmd;
}
