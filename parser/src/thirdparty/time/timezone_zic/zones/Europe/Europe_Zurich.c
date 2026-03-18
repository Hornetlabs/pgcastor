#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_Europe_Zurich[] = {
    (char*)"Rule	EU	1977	1980	-	Apr	Sun>=1	 1:00u	1:00	S",
    (char*)"Rule	EU	1977	only	-	Sep	lastSun	 1:00u	0	-",
    (char*)"Rule	EU	1978	only	-	Oct	 1	 1:00u	0	-",
    (char*)"Rule	EU	1979	1995	-	Sep	lastSun	 1:00u	0	-",
    (char*)"Rule	EU	1981	max	-	Mar	lastSun	 1:00u	1:00	S",
    (char*)"Rule	EU	1996	max	-	Oct	lastSun	 1:00u	0	-",
    (char*)"Rule	Swiss	1941	1942	-	May	Mon>=1	1:00	1:00	S",
    (char*)"Rule	Swiss	1941	1942	-	Oct	Mon>=1	2:00	0	-",
    (char*)"Zone	Europe/Zurich	0:34:08 -	LMT	1853 Jul 16 # See above comment.",
    (char*)"			0:29:46	-	BMT	1894 Jun    # Bern Mean Time",
    (char*)"			1:00	Swiss	CE%sT	1981",
    (char*)"			1:00	EU	CE%sT"};
