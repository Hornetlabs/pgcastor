#ifndef _FFTRAIL_RESET_H
#define _FFTRAIL_RESET_H

/* Serialize reset information */
bool fftrail_reset_serail(void* data, void* state);

/* Deserialize reset information */
bool fftrail_reset_deserail(void** data, void* state);

#endif
