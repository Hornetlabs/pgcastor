#include "xk_pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/xk_pg_parser_thirdparty_timezone_tzdata_info.h"

char* XK_PG_Europe_Stockholm[] =
{
    (char *)"Rule	EU	1977	1980	-	Apr	Sun>=1	 1:00u	1:00	S",
    (char *)"Rule	EU	1977	only	-	Sep	lastSun	 1:00u	0	-",
    (char *)"Rule	EU	1978	only	-	Oct	 1	 1:00u	0	-",
    (char *)"Rule	EU	1979	1995	-	Sep	lastSun	 1:00u	0	-",
    (char *)"Rule	EU	1981	max	-	Mar	lastSun	 1:00u	1:00	S",
    (char *)"Rule	EU	1996	max	-	Oct	lastSun	 1:00u	0	-",
    (char *)"Zone Europe/Stockholm	1:12:12 -	LMT	1879 Jan  1",
    (char *)"			1:00:14	-	SET	1900 Jan  1 # Swedish Time",
    (char *)"			1:00	-	CET	1916 May 14 23:00",
    (char *)"			1:00	1:00	CEST	1916 Oct  1  1:00",
    (char *)"			1:00	-	CET	1980",
    (char *)"			1:00	EU	CE%sT"
};
