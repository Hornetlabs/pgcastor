#ifndef _FFTRAIL_DATA_H
#define _FFTRAIL_DATA_H

typedef enum TRAIL_TOKENDATAHDR_ID
{
    TRAIL_TOKENDATAHDR_ID_DBMDNO = 0x00,
    TRAIL_TOKENDATAHDR_ID_TBMDNO = 0x01,
    TRAIL_TOKENDATAHDR_ID_TRANSID = 0x02,
    TRAIL_TOKENDATAHDR_ID_TRANSIND = 0x03,
    TRAIL_TOKENDATAHDR_ID_TOTALLENGTH = 0x04,
    TRAIL_TOKENDATAHDR_ID_RECLENGTH = 0x05,
    TRAIL_TOKENDATAHDR_ID_RECCOUNT = 0x06,
    TRAIL_TOKENDATAHDR_ID_FORMATTYPE = 0x07,
    TRAIL_TOKENDATAHDR_ID_TYPE = 0x08,
    TRAIL_TOKENDATAHDR_ID_ORGPOS = 0x09,
    TRAIL_TOKENDATAHDR_ID_CRC32 = 0x0A
} trail_tokendatahdr_id;

#define TRAIL_TOKENDATA_RECTAIL 0xFF

int fftrail_data_headlen(int compatibility);

/* Get length recorded in header */
uint16 fftrail_data_getreclengthfromhead(int compatibility, uint8* head);

/* Set reclength */
void fftrail_data_setreclengthonhead(int compatibility, uint8* head, uint16 reclength);

/* Get offset of real data based on record */
uint16 fftrail_data_getrecorddataoffset(int compatibility);

/* Get total length recorded in header */
uint64 fftrail_data_gettotallengthfromhead(int compatibility, uint8* head);

/* Get lsn recorded in header */
uint64 fftrail_data_getorgposfromhead(int compatibility, uint8* head);

/* Get operation type recorded in header */
uint16 fftrail_data_getsubtypefromhead(int compatibility, uint8* head);

/* Get transaction ID recorded in header */
uint8 fftrail_data_gettransindfromhead(int compatibility, uint8* head);

/*
 * Assemble header information
 */
void fftrail_data_hdrserail(ff_data* ffdatahdr, ffsmgr_state* ffstate);

/*
 * Deserialize assemble header information
 */
bool fftrail_data_hdrdeserail(ff_data* ffdatahdr, ffsmgr_state* ffstate);

/*
 * Add data to buffer
 * Parameter description:
 *  ffdatahdr               Header information, focus is that this value will be passed out
 *  ffstate                 Write cache block information
 *  ref_buffer              Cache block
 *  dtype                   Type of data to write
 *  dlen                    Length of data to write
 *  data                    Data to write
 */
bool fftrail_data_data2buffer(ff_data* ffdatahdr, ffsmgr_state* ffstate, file_buffer** ref_buffer,
                              ftrail_datatype dtype, uint64 dlen, uint8* data);

bool fftrail_data_buffer2data(ff_data* ffdatahdr, ffsmgr_state* ffstate, uint32* recoffset,
                              uint32* dataoffset, ftrail_datatype dtype, uint64 dlen, uint8* data);

/* Serialize data information */
bool fftrail_data_serail(void* data, void* state);

/* Deserialize data information */
bool fftrail_data_deserail(void** data, void* state);

/* Check if incoming data type record is beginning of a transaction */
bool fftrail_data_deserail_check_transind_start(uint8* uptr, int compatibility);

/*
 * Minimum length
 */
int fftrail_data_tokenminsize(int compatibility);

#endif
