#ifndef _RELMAPFILE_H
#define _RELMAPFILE_H

/* will relmap shoulduseto hash in */
void relmapfile_catalogdata2transcache(cache_sysdicts* sysdicts, catalogdata* catalogdata);

/* free */
void relmapfile_catalogdatafree(catalogdata* catalogdata);

#endif