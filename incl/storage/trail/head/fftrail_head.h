#ifndef _FFTRAIL_HEAD_H
#define _FFTRAIL_HEAD_H

/* Serialize header information */
bool fftrail_head_serail(void* data, void* state);

/* Deserialize information */
bool fftrail_head_deserail(void** data, void* state);

#endif
