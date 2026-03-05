#ifndef RIPPLE_UTIL_LICENSE_H
#define RIPPLE_UTIL_LICENSE_H

extern bool ripple_license_check(char *lic_path);
extern bool ripple_license_get_time(char *lic_path, uint64_t *start, uint64_t *end);

#endif
