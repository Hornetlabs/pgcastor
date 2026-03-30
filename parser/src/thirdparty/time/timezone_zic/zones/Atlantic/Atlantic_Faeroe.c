#include "pg_parser_os_incl.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"

char* PG_Atlantic_Faeroe[] = {(char*)"Rule	EU	1977	1980	-	Apr	Sun>=1	 1:00u	1:00	S",
                              (char*)"Rule	EU	1977	only	-	Sep	lastSun	 1:00u	0	-",
                              (char*)"Rule	EU	1978	only	-	Oct	 1	 1:00u	0	-",
                              (char*)"Rule	EU	1979	1995	-	Sep	lastSun	 1:00u	0	-",
                              (char*)"Rule	EU	1981	max	-	Mar	lastSun	 1:00u	1:00	S",
                              (char*)"Rule	EU	1996	max	-	Oct	lastSun	 1:00u	0	-",
                              (char*)"Zone Atlantic/Faroe	-0:27:04 -	LMT	1908 Jan 11 # Tórshavn",
                              (char*)"			 0:00	-	WET	1981",
                              (char*)"			 0:00	EU	WE%sT",
                              (char*)"Link	Atlantic/Faroe		Atlantic/Faeroe"};
