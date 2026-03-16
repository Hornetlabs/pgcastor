#ifndef _RIPPLE_FFTRAIL_DATA_H
#define _RIPPLE_FFTRAIL_DATA_H


typedef enum RIPPLE_TRAIL_TOKENDATAHDR_ID
{
    RIPPLE_TRAIL_TOKENDATAHDR_ID_DBMDNO                 = 0x00,
    RIPPLE_TRAIL_TOKENDATAHDR_ID_TBMDNO                 = 0x01,
    RIPPLE_TRAIL_TOKENDATAHDR_ID_TRANSID                = 0x02,
    RIPPLE_TRAIL_TOKENDATAHDR_ID_TRANSIND               = 0x03,
    RIPPLE_TRAIL_TOKENDATAHDR_ID_TOTALLENGTH            = 0x04,
    RIPPLE_TRAIL_TOKENDATAHDR_ID_RECLENGTH              = 0x05,
    RIPPLE_TRAIL_TOKENDATAHDR_ID_RECCOUNT               = 0x06,
    RIPPLE_TRAIL_TOKENDATAHDR_ID_FORMATTYPE             = 0x07,
    RIPPLE_TRAIL_TOKENDATAHDR_ID_TYPE                   = 0x08,
    RIPPLE_TRAIL_TOKENDATAHDR_ID_ORGPOS                 = 0x09,
    RIPPLE_TRAIL_TOKENDATAHDR_ID_CRC32                  = 0x0A
} ripple_trail_tokendatahdr_id;


#define RIPPLE_TRAIL_TOKENDATA_RECTAIL                 0xFF

int ripple_fftrail_data_headlen(int compatibility);

/* 获取头部中记录的长度 */
uint16 ripple_fftrail_data_getreclengthfromhead(int compatibility, uint8* head);

/* 设置 reclength */
void ripple_fftrail_data_setreclengthonhead(int compatibility, uint8* head, uint16 reclength);

/* 获取真实数据基于 record 的偏移 */
uint16 ripple_fftrail_data_getrecorddataoffset(int compatibility);

/* 获取头部中记录的总长度 */
uint64 ripple_fftrail_data_gettotallengthfromhead(int compatibility, uint8* head);

/* 获取头部中记录的lsn */
uint64 ripple_fftrail_data_getorgposfromhead(int compatibility, uint8* head);

/* 获取头部中记录的操作类型 */
uint16 ripple_fftrail_data_getsubtypefromhead(int compatibility, uint8* head);

/* 获取头部中记录的事务号 */
uint8 ripple_fftrail_data_gettransindfromhead(int compatibility, uint8* head);

/*
 * 组装头部信息
*/
void ripple_fftrail_data_hdrserail(ripple_ff_data* ffdatahdr, ripple_ffsmgr_state* ffstate);

/*
 * 反序列化组装头部信息
*/
bool ripple_fftrail_data_hdrdeserail(ripple_ff_data* ffdatahdr, ripple_ffsmgr_state* ffstate);

/*
 * 将数据加入到buffer中
 * 参数说明:
 *  ffdatahdr               头部信息，关注点为此值会待出去
 *  ffstate                 写缓存块的信息
 *  ref_buffer              缓存块
 *  dtype                   待写数据的类型
 *  dlen                    待写数据的长度
 *  data                    待写数据
*/
bool ripple_fftrail_data_data2buffer(ripple_ff_data* ffdatahdr,
                                        ripple_ffsmgr_state* ffstate,
                                        ripple_file_buffer** ref_buffer,
                                        ripple_ftrail_datatype dtype,
                                        uint64 dlen,
                                        uint8* data);

bool ripple_fftrail_data_buffer2data(ripple_ff_data* ffdatahdr, 
                                        ripple_ffsmgr_state* ffstate,
                                        uint32* recoffset,
                                        uint32* dataoffset,
                                        ripple_ftrail_datatype dtype,
                                        uint64 dlen,
                                        uint8* data);

/* 序列化数据信息 */
bool ripple_fftrail_data_serail(void* data, void* state);

/* 反序列化数据信息 */
bool ripple_fftrail_data_deserail(void** data, void* state);

/* 判断传入的data类型的record的是否为一个事务的开始 */
bool ripple_fftrail_data_deserail_check_transind_start(uint8 *uptr, int compatibility);

/*
 * 最小长度
*/
int ripple_fftrail_data_tokenminsize(int compatibility);


#endif
