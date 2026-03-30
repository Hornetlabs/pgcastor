#ifndef _VARSTR_H_
#define _VARSTR_H_

typedef struct VARSTR
{
    /* Used length */
    uint64_t start;

    /* data length */
    uint64_t size;

    /* Data area */
    uint8_t* data;
} varstr;

/* Initialize */
extern varstr* varstr_init(uint64 len);

/* Reset */
extern bool varstr_reset(varstr* vstr);

/*
 * Expand
 *  No length and input parameter check
 */
extern bool varstr_enlarge(varstr* vstr, uint64 needed);

/* Add content */
extern bool varstr_append(varstr* vstr, const char* fmt, ...);

/* Add string */
extern bool varstr_appendbinary(varstr* vstr, const char* data, uint64 datalen);

/* Merge string */
extern bool varstr_appendstr(varstr* vstr, const char* data);

/* Add character */
extern bool varstr_appendchar(varstr* vstr, char ch);

/* Release */
extern void varstr_free(varstr* vstr);

#endif
