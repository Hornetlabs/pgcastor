#ifndef _RIPPLE_RELMAPFILE_H
#define _RIPPLE_RELMAPFILE_H

/* 将 relmap 应用到 hash 中 */
void ripple_relmapfile_catalogdata2transcache(ripple_cache_sysdicts* sysdicts,
                                              ripple_catalogdata* catalogdata);

/* 释放 */
void ripple_relmapfile_catalogdatafree(ripple_catalogdata* catalogdata);

#endif