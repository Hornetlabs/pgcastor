#ifndef CRC_CHECK_H
#define CRC_CHECK_H

#define INIT_CRC32C(crc) ((crc) = 0xFFFFFFFF)
#define EQ_CRC32C(c1, c2) ((c1) == (c2))

extern pg_crc32c pg_comp_crc32c_sb8(pg_crc32c crc, const void *data, size_t len);

#define COMP_CRC32C(crc, data, len) \
    ((crc) = pg_comp_crc32c_sb8((crc), (data), (len)))
#define FIN_CRC32C(crc) ((crc) ^= 0xFFFFFFFF)

#endif
