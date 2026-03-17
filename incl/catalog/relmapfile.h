#ifndef _RELMAPFILE_H
#define _RELMAPFILE_H

/* 将 relmap 应用到 hash 中 */
void relmapfile_catalogdata2transcache(cache_sysdicts* sysdicts,
                                              catalogdata* catalogdata);

/* 释放 */
void relmapfile_catalogdatafree(catalogdata* catalogdata);

#endif