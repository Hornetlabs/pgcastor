#ifndef RIPPLE_UUID_H
#define RIPPLE_UUID_H

/* uuid size in bytes */
#define RIPPLE_UUID_LEN 16

typedef struct ripple_uuid_t
{
    unsigned char data[RIPPLE_UUID_LEN];
} ripple_uuid_t;

extern ripple_uuid_t *ripple_uuid_init(void);
extern ripple_uuid_t *ripple_random_uuid(void);
extern ripple_uuid_t *ripple_uuid_copy(ripple_uuid_t *uuid);
extern void ripple_uuid_free(ripple_uuid_t *uuid);
extern char *uuid2string(ripple_uuid_t *uuid);
#endif
