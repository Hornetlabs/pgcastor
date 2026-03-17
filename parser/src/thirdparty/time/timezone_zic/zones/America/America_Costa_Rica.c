#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_America_Costa_Rica[] =
{
    (char *)"Rule	CR	1979	1980	-	Feb	lastSun	0:00	1:00	D",
    (char *)"Rule	CR	1979	1980	-	Jun	Sun>=1	0:00	0	S",
    (char *)"Rule	CR	1991	1992	-	Jan	Sat>=15	0:00	1:00	D",
    (char *)"Rule	CR	1991	only	-	Jul	 1	0:00	0	S",
    (char *)"Rule	CR	1992	only	-	Mar	15	0:00	0	S",
    (char *)"Zone America/Costa_Rica	-5:36:13 -	LMT	1890        # San José",
    (char *)"			-5:36:13 -	SJMT	1921 Jan 15 # San José Mean Time",
    (char *)"			-6:00	CR	C%sT"
};
