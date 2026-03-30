#ifndef _FORMAT_H
#define _FORMAT_H

static inline void put64bit(uint8_t** ptr, uint64_t val)
{
    (*ptr)[0] = ((val) >> 56) & 0xFF;
    (*ptr)[1] = ((val) >> 48) & 0xFF;
    (*ptr)[2] = ((val) >> 40) & 0xFF;
    (*ptr)[3] = ((val) >> 32) & 0xFF;
    (*ptr)[4] = ((val) >> 24) & 0xFF;
    (*ptr)[5] = ((val) >> 16) & 0xFF;
    (*ptr)[6] = ((val) >> 8) & 0xFF;
    (*ptr)[7] = (val) & 0xFF;
    (*ptr) += 8;
}

static inline void put32bit(uint8_t** ptr, uint32_t val)
{
    (*ptr)[0] = ((val) >> 24) & 0xFF;
    (*ptr)[1] = ((val) >> 16) & 0xFF;
    (*ptr)[2] = ((val) >> 8) & 0xFF;
    (*ptr)[3] = (val) & 0xFF;
    (*ptr) += 4;
}

static inline void put16bit(uint8_t** ptr, uint16_t val)
{
    (*ptr)[0] = ((val) >> 8) & 0xFF;
    (*ptr)[1] = (val) & 0xFF;
    (*ptr) += 2;
}

static inline void put8bit(uint8_t** ptr, uint8_t val)
{
    (*ptr)[0] = (val) & 0xFF;
    (*ptr)++;
}

static inline uint64_t get64bit(uint8_t** ptr)
{
    uint64_t t64 = 0;
    t64 = ((uint32_t)(*ptr)[3] +
           (uint32_t)256 *
               ((uint32_t)(*ptr)[2] + (uint32_t)256 * ((uint32_t)(*ptr)[1] + (uint32_t)256 * (uint32_t)(*ptr)[0])));
    t64 <<= 32;
    t64 |= (((uint32_t)(*ptr)[7] +
             (uint32_t)256 *
                 ((uint32_t)(*ptr)[6] + (uint32_t)256 * ((uint32_t)(*ptr)[5] + (uint32_t)256 * (uint32_t)(*ptr)[4])))) &
           0xffffffffU;
    (*ptr) += 8;
    return t64;
}

static inline uint32_t get32bit(uint8_t** ptr)
{
    uint32_t t32;
    t32 = ((uint32_t)(*ptr)[3] +
           (uint32_t)256 *
               ((uint32_t)(*ptr)[2] + (uint32_t)256 * ((uint32_t)(*ptr)[1] + (uint32_t)256 * (uint32_t)(*ptr)[0])));
    (*ptr) += 4;
    return t32;
}

static inline uint16_t get16bit(uint8_t** ptr)
{
    uint32_t t16;
    t16 = (*ptr)[1] + 256 * (*ptr)[0];
    (*ptr) += 2;
    return t16;
}

static inline uint8_t get8bit(uint8_t** ptr)
{
    uint32_t t8;
    t8 = (*ptr)[0];
    (*ptr)++;
    return t8;
}

#endif
