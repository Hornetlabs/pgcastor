#ifndef _FFTRAIL_TAIL_H
#define _FFTRAIL_TAIL_H

/* Serialize tail information */
bool fftrail_tail_serail(void* data, void* state);

/* Deserialize tail information */
bool fftrail_tail_deserail(void** data, void* state);

#endif
