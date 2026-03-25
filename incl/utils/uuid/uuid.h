#ifndef UUID_H
#define UUID_H

/* uuid size in bytes */
#define UUID_LEN 16

typedef struct uuid_t
{
    unsigned char data[UUID_LEN];
} uuid_t;

extern uuid_t* uuid_init(void);
extern uuid_t* random_uuid(void);
extern uuid_t* uuid_copy(uuid_t* uuid);
extern void    uuid_free(uuid_t* uuid);
extern char*   uuid2string(uuid_t* uuid);
#endif
