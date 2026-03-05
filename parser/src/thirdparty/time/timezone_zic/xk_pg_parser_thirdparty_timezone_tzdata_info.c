/**
 * @file                xk_pg_parser_thirdparty_timezone_tzdata_info.cpp
 * @author              ByteSynch
 * @brief               时区定位函数的实现
 * @version             0.1
 * @date                2023-09-19
 *
 * @copyright           Copyright (c) 2023
 *
 */

#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "thirdparty/time/timezone_zic/xk_pg_parser_thirdparty_timezone_tzdata_info.h"

typedef struct LOCAL_TZDATA_DISPATCH_STRUCT
{
    char*                         tz_name;
    char**                        tz_data;
    int                           tz_data_size;
} local_tzdata_dispatch_struct;

static local_tzdata_dispatch_struct africa_tzdata_info[] =
{
    { (char*)"Africa/Abidjan", XK_PG_Africa_Abidjan, 3 },
    { (char*)"Africa/Accra", XK_PG_Africa_Accra, 5 },
    { (char*)"Africa/Addis_Ababa", XK_PG_Africa_Addis_Ababa, 7 },
    { (char*)"Africa/Algiers", XK_PG_Africa_Algiers, 33 },
    { (char*)"Africa/Asmara", XK_PG_Africa_Asmara, 7 },
    { (char*)"Africa/Asmera", XK_PG_Africa_Asmera, 7 },
    { (char*)"Africa/Bamako", XK_PG_Africa_Bamako, 4 },
    { (char*)"Africa/Bangui", XK_PG_Africa_Bangui, 4 },
    { (char*)"Africa/Banjul", XK_PG_Africa_Banjul, 4 },
    { (char*)"Africa/Bissau", XK_PG_Africa_Bissau, 4 },
    { (char*)"Africa/Blantyre", XK_PG_Africa_Blantyre, 4 },
    { (char*)"Africa/Brazzaville", XK_PG_Africa_Brazzaville, 4 },
    { (char*)"Africa/Bujumbura", XK_PG_Africa_Bujumbura, 4 },
    { (char*)"Africa/Cairo", XK_PG_Africa_Cairo, 35 },
    { (char*)"Africa/Casablanca", XK_PG_Africa_Casablanca, 189 },
    { (char*)"Africa/Ceuta", XK_PG_Africa_Ceuta, 55 },
    { (char*)"Africa/Conakry", XK_PG_Africa_Conakry, 4 },
    { (char*)"Africa/Dakar", XK_PG_Africa_Dakar, 4 },
    { (char*)"Africa/Dar_es_Salaam", XK_PG_Africa_Dar_es_Salaam, 7 },
    { (char*)"Africa/Djibouti", XK_PG_Africa_Djibouti, 7 },
    { (char*)"Africa/Douala", XK_PG_Africa_Douala, 4 },
    { (char*)"Africa/El_Aaiun", XK_PG_Africa_El_Aaiun, 188 },
    { (char*)"Africa/Freetown", XK_PG_Africa_Freetown, 4 },
    { (char*)"Africa/Gaborone", XK_PG_Africa_Gaborone, 4 },
    { (char*)"Africa/Harare", XK_PG_Africa_Harare, 4 },
    { (char*)"Africa/Johannesburg", XK_PG_Africa_Johannesburg, 6 },
    { (char*)"Africa/Juba", XK_PG_Africa_Juba, 8 },
    { (char*)"Africa/Kampala", XK_PG_Africa_Kampala, 7 },
    { (char*)"Africa/Khartoum", XK_PG_Africa_Khartoum, 9 },
    { (char*)"Africa/Kigali", XK_PG_Africa_Kigali, 4 },
    { (char*)"Africa/Kinshasa", XK_PG_Africa_Kinshasa, 4 },
    { (char*)"Africa/Lagos", XK_PG_Africa_Lagos, 3 },
    { (char*)"Africa/Libreville", XK_PG_Africa_Libreville, 4 },
    { (char*)"Africa/Lome", XK_PG_Africa_Lome, 4 },
    { (char*)"Africa/Luanda", XK_PG_Africa_Luanda, 4 },
    { (char*)"Africa/Lubumbashi", XK_PG_Africa_Lubumbashi, 4 },
    { (char*)"Africa/Lusaka", XK_PG_Africa_Lusaka, 4 },
    { (char*)"Africa/Malabo", XK_PG_Africa_Malabo, 4 },
    { (char*)"Africa/Maputo", XK_PG_Africa_Maputo, 3 },
    { (char*)"Africa/Maseru", XK_PG_Africa_Maseru, 7 },
    { (char*)"Africa/Mbabane", XK_PG_Africa_Mbabane, 7 },
    { (char*)"Africa/Mogadishu", XK_PG_Africa_Mogadishu, 7 },
    { (char*)"Africa/Monrovia", XK_PG_Africa_Monrovia, 5 },
    { (char*)"Africa/Nairobi", XK_PG_Africa_Nairobi, 6 },
    { (char*)"Africa/Ndjamena", XK_PG_Africa_Ndjamena, 5 },
    { (char*)"Africa/Niamey", XK_PG_Africa_Niamey, 4 },
    { (char*)"Africa/Nouakchott", XK_PG_Africa_Nouakchott, 4 },
    { (char*)"Africa/Ouagadougou", XK_PG_Africa_Ouagadougou, 4 },
    { (char*)"Africa/Porto-Novo", XK_PG_Africa_Porto_Novo, 4 },
    { (char*)"Africa/Sao_Tome", XK_PG_Africa_Sao_Tome, 6 },
    { (char*)"Africa/Timbuktu", XK_PG_Africa_Timbuktu, 4 },
    { (char*)"Africa/Tripoli", XK_PG_Africa_Tripoli, 27 },
    { (char*)"Africa/Tunis", XK_PG_Africa_Tunis, 29 },
    { (char*)"Africa/Windhoek", XK_PG_Africa_Windhoek, 10 }
};

static local_tzdata_dispatch_struct america_tzdata_info[] =
{
    { (char*)"America/Adak", XK_PG_America_Adak, 23 },
    { (char*)"America/Anchorage", XK_PG_America_Anchorage, 22 },
    { (char*)"America/Anguilla", XK_PG_America_Anguilla, 4 },
    { (char*)"America/Antigua", XK_PG_America_Antigua, 4 },
    { (char*)"America/Araguaina", XK_PG_America_Araguaina, 65 },
    { (char*)"America/Argentina/Buenos_Aires", XK_PG_America_Argentina_Buenos_Aires, 37 },
    { (char*)"America/Argentina/Catamarca", XK_PG_America_Argentina_Catamarca, 42 },
    { (char*)"America/Argentina/ComodRivadavia", XK_PG_America_Argentina_ComodRivadavia, 43 },
    { (char*)"America/Argentina/Cordoba", XK_PG_America_Argentina_Cordoba, 39 },
    { (char*)"America/Argentina/Jujuy", XK_PG_America_Argentina_Jujuy, 43 },
    { (char*)"America/Argentina/La_Rioja", XK_PG_America_Argentina_La_Rioja, 42 },
    { (char*)"America/Argentina/Mendoza", XK_PG_America_Argentina_Mendoza, 46 },
    { (char*)"America/Argentina/Rio_Gallegos", XK_PG_America_Argentina_Rio_Gallegos, 40 },
    { (char*)"America/Argentina/Salta", XK_PG_America_Argentina_Salta, 40 },
    { (char*)"America/Argentina/San_Juan", XK_PG_America_Argentina_San_Juan, 42 },
    { (char*)"America/Argentina/San_Luis", XK_PG_America_Argentina_San_Luis, 48 },
    { (char*)"America/Argentina/Tucuman", XK_PG_America_Argentina_Tucuman, 41 },
    { (char*)"America/Argentina/Ushuaia", XK_PG_America_Argentina_Ushuaia, 40 },
    { (char*)"America/Aruba", XK_PG_America_Aruba, 5 },
    { (char*)"America/Asuncion", XK_PG_America_Asuncion, 28 },
    { (char*)"America/Atikokan", XK_PG_America_Atikokan, 16 },
    { (char*)"America/Atka", XK_PG_America_Atka, 24 },
    { (char*)"America/Bahia", XK_PG_America_Bahia, 63 },
    { (char*)"America/Bahia_Banderas", XK_PG_America_Bahia_Banderas, 26 },
    { (char*)"America/Barbados", XK_PG_America_Barbados, 9 },
    { (char*)"America/Belem", XK_PG_America_Belem, 61 },
    { (char*)"America/Belize", XK_PG_America_Belize, 9 },
    { (char*)"America/Blanc-Sablon", XK_PG_America_Blanc_Sablon, 14 },
    { (char*)"America/Boa_Vista", XK_PG_America_Boa_Vista, 63 },
    { (char*)"America/Bogota", XK_PG_America_Bogota, 6 },
    { (char*)"America/Boise", XK_PG_America_Boise, 19 },
    { (char*)"America/Buenos_Aires", XK_PG_America_Buenos_Aires, 38 },
    { (char*)"America/Cambridge_Bay", XK_PG_America_Cambridge_Bay, 29 },
    { (char*)"America/Campo_Grande", XK_PG_America_Campo_Grande, 60 },
    { (char*)"America/Cancun", XK_PG_America_Cancun, 20 },
    { (char*)"America/Caracas", XK_PG_America_Caracas, 7 },
    { (char*)"America/Catamarca", XK_PG_America_Catamarca, 43 },
    { (char*)"America/Cayenne", XK_PG_America_Cayenne, 4 },
    { (char*)"America/Cayman", XK_PG_America_Cayman, 5 },
    { (char*)"America/Chicago", XK_PG_America_Chicago, 28 },
    { (char*)"America/Chihuahua", XK_PG_America_Chihuahua, 25 },
    { (char*)"America/Coral_Harbour", XK_PG_America_Coral_Harbour, 17 },
    { (char*)"America/Cordoba", XK_PG_America_Cordoba, 40 },
    { (char*)"America/Costa_Rica", XK_PG_America_Costa_Rica, 9 },
    { (char*)"America/Creston", XK_PG_America_Creston, 5 },
    { (char*)"America/Cuiaba", XK_PG_America_Cuiaba, 62 },
    { (char*)"America/Curacao", XK_PG_America_Curacao, 4 },
    { (char*)"America/Danmarkshavn", XK_PG_America_Danmarkshavn, 11 },
    { (char*)"America/Dawson", XK_PG_America_Dawson, 28 },
    { (char*)"America/Dawson_Creek", XK_PG_America_Dawson_Creek, 24 },
    { (char*)"America/Denver", XK_PG_America_Denver, 25 },
    { (char*)"America/Detroit", XK_PG_America_Detroit, 26 },
    { (char*)"America/Dominica", XK_PG_America_Dominica, 4 },
    { (char*)"America/Edmonton", XK_PG_America_Edmonton, 27 },
    { (char*)"America/Eirunepe", XK_PG_America_Eirunepe, 65 },
    { (char*)"America/El_Salvador", XK_PG_America_El_Salvador, 5 },
    { (char*)"America/Ensenada", XK_PG_America_Ensenada, 53 },
    { (char*)"America/Fortaleza", XK_PG_America_Fortaleza, 65 },
    { (char*)"America/Fort_Nelson", XK_PG_America_Fort_Nelson, 26 },
    { (char*)"America/Fort_Wayne", XK_PG_America_Fort_Wayne, 29 },
    { (char*)"America/Glace_Bay", XK_PG_America_Glace_Bay, 58 },
    { (char*)"America/Godthab", XK_PG_America_Godthab, 11 },
    { (char*)"America/Goose_Bay", XK_PG_America_Goose_Bay, 40 },
    { (char*)"America/Grand_Turk", XK_PG_America_Grand_Turk, 20 },
    { (char*)"America/Grenada", XK_PG_America_Grenada, 4 },
    { (char*)"America/Guadeloupe", XK_PG_America_Guadeloupe, 4 },
    { (char*)"America/Guatemala", XK_PG_America_Guatemala, 11 },
    { (char*)"America/Guayaquil", XK_PG_America_Guayaquil, 6 },
    { (char*)"America/Guyana", XK_PG_America_Guyana, 5 },
    { (char*)"America/Halifax", XK_PG_America_Halifax, 59 },
    { (char*)"America/Havana", XK_PG_America_Havana, 43 },
    { (char*)"America/Hermosillo", XK_PG_America_Hermosillo, 26 },
    { (char*)"America/Indiana/Indianapolis", XK_PG_America_Indiana_Indianapolis, 28 },
    { (char*)"America/Indiana/Knox", XK_PG_America_Indiana_Knox, 26 },
    { (char*)"America/Indiana/Marengo", XK_PG_America_Indiana_Marengo, 27 },
    { (char*)"America/Indiana/Petersburg", XK_PG_America_Indiana_Petersburg, 26 },
    { (char*)"America/Indiana/Tell_City", XK_PG_America_Indiana_Tell_City, 26 },
    { (char*)"America/Indiana/Vevay", XK_PG_America_Indiana_Vevay, 20 },
    { (char*)"America/Indiana/Vincennes", XK_PG_America_Indiana_Vincennes, 31 },
    { (char*)"America/Indiana/Winamac", XK_PG_America_Indiana_Winamac, 26 },
    { (char*)"America/Indianapolis", XK_PG_America_Indianapolis, 29 },
    { (char*)"America/Inuvik", XK_PG_America_Inuvik, 27 },
    { (char*)"America/Iqaluit", XK_PG_America_Iqaluit, 27 },
    { (char*)"America/Jamaica", XK_PG_America_Jamaica, 19 },
    { (char*)"America/Jujuy", XK_PG_America_Jujuy, 44 },
    { (char*)"America/Juneau", XK_PG_America_Juneau, 24 },
    { (char*)"America/Kentucky/Louisville", XK_PG_America_Kentucky_Louisville, 32 },
    { (char*)"America/Kentucky/Monticello", XK_PG_America_Kentucky_Monticello, 19 },
    { (char*)"America/Knox_IN", XK_PG_America_Knox_IN, 27 },
    { (char*)"America/Kralendijk", XK_PG_America_Kralendijk, 5 },
    { (char*)"America/La_Paz", XK_PG_America_La_Paz, 5 },
    { (char*)"America/Lima", XK_PG_America_Lima, 14 },
    { (char*)"America/Los_Angeles", XK_PG_America_Los_Angeles, 23 },
    { (char*)"America/Louisville", XK_PG_America_Louisville, 33 },
    { (char*)"America/Lower_Princes", XK_PG_America_Lower_Princes, 5 },
    { (char*)"America/Maceio", XK_PG_America_Maceio, 67 },
    { (char*)"America/Managua", XK_PG_America_Managua, 16 },
    { (char*)"America/Manaus", XK_PG_America_Manaus, 63 },
    { (char*)"America/Marigot", XK_PG_America_Marigot, 4 },
    { (char*)"America/Martinique", XK_PG_America_Martinique, 6 },
    { (char*)"America/Matamoros", XK_PG_America_Matamoros, 33 },
    { (char*)"America/Mazatlan", XK_PG_America_Mazatlan, 25 },
    { (char*)"America/Mendoza", XK_PG_America_Mendoza, 47 },
    { (char*)"America/Menominee", XK_PG_America_Menominee, 23 },
    { (char*)"America/Merida", XK_PG_America_Merida, 19 },
    { (char*)"America/Metlakatla", XK_PG_America_Metlakatla, 24 },
    { (char*)"America/Mexico_City", XK_PG_America_Mexico_City, 24 },
    { (char*)"America/Miquelon", XK_PG_America_Miquelon, 15 },
    { (char*)"America/Moncton", XK_PG_America_Moncton, 33 },
    { (char*)"America/Monterrey", XK_PG_America_Monterrey, 32 },
    { (char*)"America/Montevideo", XK_PG_America_Montevideo, 60 },
    { (char*)"America/Montreal", XK_PG_America_Montreal, 40 },
    { (char*)"America/Montserrat", XK_PG_America_Montserrat, 4 },
    { (char*)"America/Nassau", XK_PG_America_Nassau, 19 },
    { (char*)"America/New_York", XK_PG_America_New_York, 25 },
    { (char*)"America/Nipigon", XK_PG_America_Nipigon, 15 },
    { (char*)"America/Nome", XK_PG_America_Nome, 23 },
    { (char*)"America/Noronha", XK_PG_America_Noronha, 65 },
    { (char*)"America/North_Dakota/Beulah", XK_PG_America_North_Dakota_Beulah, 17 },
    { (char*)"America/North_Dakota/Center", XK_PG_America_North_Dakota_Center, 17 },
    { (char*)"America/North_Dakota/New_Salem", XK_PG_America_North_Dakota_New_Salem, 17 },
    { (char*)"America/Nuuk", XK_PG_America_Nuuk, 10 },
    { (char*)"America/Ojinaga", XK_PG_America_Ojinaga, 39 },
    { (char*)"America/Panama", XK_PG_America_Panama, 4 },
    { (char*)"America/Pangnirtung", XK_PG_America_Pangnirtung, 28 },
    { (char*)"America/Paramaribo", XK_PG_America_Paramaribo, 6 },
    { (char*)"America/Phoenix", XK_PG_America_Phoenix, 21 },
    { (char*)"America/Port-au-Prince", XK_PG_America_Port_au_Prince, 15 },
    { (char*)"America/Porto_Acre", XK_PG_America_Porto_Acre, 64 },
    { (char*)"America/Port_of_Spain", XK_PG_America_Port_of_Spain, 3 },
    { (char*)"America/Porto_Velho", XK_PG_America_Porto_Velho, 61 },
    { (char*)"America/Puerto_Rico", XK_PG_America_Puerto_Rico, 18 },
    { (char*)"America/Punta_Arenas", XK_PG_America_Punta_Arenas, 44 },
    { (char*)"America/Rainy_River", XK_PG_America_Rainy_River, 15 },
    { (char*)"America/Rankin_Inlet", XK_PG_America_Rankin_Inlet, 27 },
    { (char*)"America/Recife", XK_PG_America_Recife, 65 },
    { (char*)"America/Regina", XK_PG_America_Regina, 21 },
    { (char*)"America/Resolute", XK_PG_America_Resolute, 29 },
    { (char*)"America/Rio_Branco", XK_PG_America_Rio_Branco, 63 },
    { (char*)"America/Rosario", XK_PG_America_Rosario, 40 },
    { (char*)"America/Santa_Isabel", XK_PG_America_Santa_Isabel, 53 },
    { (char*)"America/Santarem", XK_PG_America_Santarem, 62 },
    { (char*)"America/Santiago", XK_PG_America_Santiago, 43 },
    { (char*)"America/Santo_Domingo", XK_PG_America_Santo_Domingo, 26 },
    { (char*)"America/Sao_Paulo", XK_PG_America_Sao_Paulo, 62 },
    { (char*)"America/Scoresbysund", XK_PG_America_Scoresbysund, 28 },
    { (char*)"America/Shiprock", XK_PG_America_Shiprock, 26 },
    { (char*)"America/Sitka", XK_PG_America_Sitka, 22 },
    { (char*)"America/St_Barthelemy", XK_PG_America_St_Barthelemy, 4 },
    { (char*)"America/St_Johns", XK_PG_America_St_Johns, 38 },
    { (char*)"America/St_Kitts", XK_PG_America_St_Kitts, 4 },
    { (char*)"America/St_Lucia", XK_PG_America_St_Lucia, 4 },
    { (char*)"America/St_Thomas", XK_PG_America_St_Thomas, 4 },
    { (char*)"America/St_Vincent", XK_PG_America_St_Vincent, 4 },
    { (char*)"America/Swift_Current", XK_PG_America_Swift_Current, 38 },
    { (char*)"America/Tegucigalpa", XK_PG_America_Tegucigalpa, 7 },
    { (char*)"America/Thule", XK_PG_America_Thule, 9 },
    { (char*)"America/Thunder_Bay", XK_PG_America_Thunder_Bay, 40 },
    { (char*)"America/Tijuana", XK_PG_America_Tijuana, 52 },
    { (char*)"America/Toronto", XK_PG_America_Toronto, 39 },
    { (char*)"America/Tortola", XK_PG_America_Tortola, 4 },
    { (char*)"America/Vancouver", XK_PG_America_Vancouver, 23 },
    { (char*)"America/Virgin", XK_PG_America_Virgin, 4 },
    { (char*)"America/Whitehorse", XK_PG_America_Whitehorse, 28 },
    { (char*)"America/Winnipeg", XK_PG_America_Winnipeg, 38 },
    { (char*)"America/Yakutat", XK_PG_America_Yakutat, 21 },
    { (char*)"America/Yellowknife", XK_PG_America_Yellowknife, 26 }
};

static local_tzdata_dispatch_struct antartica_tzdata_info[] =
{
    { (char*)"Antarctica/Casey", XK_PG_Antarctica_Casey, 14 },
    { (char*)"Antarctica/Davis", XK_PG_Antarctica_Davis, 9 },
    { (char*)"Antarctica/DumontDUrville", XK_PG_Antarctica_DumontDUrville, 5 },
    { (char*)"Antarctica/Macquarie", XK_PG_Antarctica_Macquarie, 36 },
    { (char*)"Antarctica/Mawson", XK_PG_Antarctica_Mawson, 4 },
    { (char*)"Antarctica/McMurdo", XK_PG_Antarctica_McMurdo, 21 },
    { (char*)"Antarctica/Palmer", XK_PG_Antarctica_Palmer, 69 },
    { (char*)"Antarctica/Rothera", XK_PG_Antarctica_Rothera, 3 },
    { (char*)"Antarctica/South_Pole", XK_PG_Antarctica_South_Pole, 18 },
    { (char*)"Antarctica/Syowa", XK_PG_Antarctica_Syowa, 3 },
    { (char*)"Antarctica/Troll", XK_PG_Antarctica_Troll, 4 },
    { (char*)"Antarctica/Vostok", XK_PG_Antarctica_Vostok, 3 }
};

static local_tzdata_dispatch_struct arctic_tzdata_info[] =
{
    { (char*)"Arctic/Longyearbyen", XK_PG_Arctic_Longyearbyen, 37 }
};

static local_tzdata_dispatch_struct asia_tzdata_info[] =
{
    { (char*)"Asia/Aden", XK_PG_Asia_Aden, 4 },
    { (char*)"Asia/Almaty", XK_PG_Asia_Almaty, 12 },
    { (char*)"Asia/Amman", XK_PG_Asia_Amman, 34 },
    { (char*)"Asia/Anadyr", XK_PG_Asia_Anadyr, 25 },
    { (char*)"Asia/Aqtau", XK_PG_Asia_Aqtau, 15 },
    { (char*)"Asia/Aqtobe", XK_PG_Asia_Aqtobe, 15 },
    { (char*)"Asia/Ashgabat", XK_PG_Asia_Ashgabat, 11 },
    { (char*)"Asia/Ashkhabad", XK_PG_Asia_Ashkhabad, 12 },
    { (char*)"Asia/Atyrau", XK_PG_Asia_Atyrau, 15 },
    { (char*)"Asia/Baghdad", XK_PG_Asia_Baghdad, 13 },
    { (char*)"Asia/Bahrain", XK_PG_Asia_Bahrain, 5 },
    { (char*)"Asia/Baku", XK_PG_Asia_Baku, 18 },
    { (char*)"Asia/Bangkok", XK_PG_Asia_Bangkok, 4 },
    { (char*)"Asia/Barnaul", XK_PG_Asia_Barnaul, 26 },
    { (char*)"Asia/Beijing", XK_PG_Asia_Beijing, 5 },
    { (char*)"Asia/Beirut", XK_PG_Asia_Beirut, 27 },
    { (char*)"Asia/Bishkek", XK_PG_Asia_Bishkek, 16 },
    { (char*)"Asia/Brunei", XK_PG_Asia_Brunei, 4 },
    { (char*)"Asia/Calcutta", XK_PG_Asia_Calcutta, 10 },
    { (char*)"Asia/Choibalsan", XK_PG_Asia_Choibalsan, 15 },
    { (char*)"Asia/Chongqing", XK_PG_Asia_Chongqing, 22 },
    { (char*)"Asia/Chungking", XK_PG_Asia_Chungking, 22 },
    { (char*)"Asia/Colombo", XK_PG_Asia_Colombo, 10 },
    { (char*)"Asia/Dacca", XK_PG_Asia_Dacca, 11 },
    { (char*)"Asia/Damascus", XK_PG_Asia_Damascus, 44 },
    { (char*)"Asia/Dhaka", XK_PG_Asia_Dhaka, 11 },
    { (char*)"Asia/Dili", XK_PG_Asia_Dili, 6 },
    { (char*)"Asia/Dubai", XK_PG_Asia_Dubai, 3 },
    { (char*)"Asia/Dushanbe", XK_PG_Asia_Dushanbe, 11 },
    { (char*)"Asia/Famagusta", XK_PG_Asia_Famagusta, 18 },
    { (char*)"Asia/Gaza", XK_PG_Asia_Gaza, 161 },
    { (char*)"Asia/Harbin", XK_PG_Asia_Harbin, 22 },
    { (char*)"Asia/Hebron", XK_PG_Asia_Hebron, 155 },
    { (char*)"Asia/Ho_Chi_Minh", XK_PG_Asia_Ho_Chi_Minh, 11 },
    { (char*)"Asia/Hong_Kong", XK_PG_Asia_Hong_Kong, 21 },
    { (char*)"Asia/Hovd", XK_PG_Asia_Hovd, 13 },
    { (char*)"Asia/Irkutsk", XK_PG_Asia_Irkutsk, 25 },
    { (char*)"Asia/Istanbul", XK_PG_Asia_Istanbul, 66 },
    { (char*)"Asia/Jakarta", XK_PG_Asia_Jakarta, 10 },
    { (char*)"Asia/Jayapura", XK_PG_Asia_Jayapura, 5 },
    { (char*)"Asia/Jerusalem", XK_PG_Asia_Jerusalem, 91 },
    { (char*)"Asia/Kabul", XK_PG_Asia_Kabul, 4 },
    { (char*)"Asia/Kamchatka", XK_PG_Asia_Kamchatka, 24 },
    { (char*)"Asia/Karachi", XK_PG_Asia_Karachi, 12 },
    { (char*)"Asia/Kashgar", XK_PG_Asia_Kashgar, 8 },
    { (char*)"Asia/Kathmandu", XK_PG_Asia_Kathmandu, 4 },
    { (char*)"Asia/Katmandu", XK_PG_Asia_Katmandu, 5 },
    { (char*)"Asia/Khandyga", XK_PG_Asia_Khandyga, 26 },
    { (char*)"Asia/Kolkata", XK_PG_Asia_Kolkata, 10 },
    { (char*)"Asia/Krasnoyarsk", XK_PG_Asia_Krasnoyarsk, 24 },
    { (char*)"Asia/Kuala_Lumpur", XK_PG_Asia_Kuala_Lumpur, 10 },
    { (char*)"Asia/Kuching", XK_PG_Asia_Kuching, 8 },
    { (char*)"Asia/Kuwait", XK_PG_Asia_Kuwait, 4 },
    { (char*)"Asia/Macao", XK_PG_Asia_Macao, 33 },
    { (char*)"Asia/Macau", XK_PG_Asia_Macau, 32 },
    { (char*)"Asia/Magadan", XK_PG_Asia_Magadan, 25 },
    { (char*)"Asia/Makassar", XK_PG_Asia_Makassar, 6 },
    { (char*)"Asia/Manila", XK_PG_Asia_Manila, 12 },
    { (char*)"Asia/Muscat", XK_PG_Asia_Muscat, 4 },
    { (char*)"Asia/Nicosia", XK_PG_Asia_Nicosia, 16 },
    { (char*)"Asia/Novokuznetsk", XK_PG_Asia_Novokuznetsk, 24 },
    { (char*)"Asia/Novosibirsk", XK_PG_Asia_Novosibirsk, 26 },
    { (char*)"Asia/Omsk", XK_PG_Asia_Omsk, 24 },
    { (char*)"Asia/Oral", XK_PG_Asia_Oral, 16 },
    { (char*)"Asia/Phnom_Penh", XK_PG_Asia_Phnom_Penh, 5 },
    { (char*)"Asia/Pontianak", XK_PG_Asia_Pontianak, 10 },
    { (char*)"Asia/Pyongyang", XK_PG_Asia_Pyongyang, 7 },
    { (char*)"Asia/Qatar", XK_PG_Asia_Qatar, 4 },
    { (char*)"Asia/Qostanay", XK_PG_Asia_Qostanay, 15 },
    { (char*)"Asia/Qyzylorda", XK_PG_Asia_Qyzylorda, 18 },
    { (char*)"Asia/Rangoon", XK_PG_Asia_Rangoon, 6 },
    { (char*)"Asia/Riyadh", XK_PG_Asia_Riyadh, 3 },
    { (char*)"Asia/Riyadh87", XK_PG_Asia_Riyadh87, 369 },
    { (char*)"Asia/Riyadh88", XK_PG_Asia_Riyadh88, 370 },
    { (char*)"Asia/Riyadh89", XK_PG_Asia_Riyadh89, 369 },
    { (char*)"Asia/Saigon", XK_PG_Asia_Saigon, 12 },
    { (char*)"Asia/Sakhalin", XK_PG_Asia_Sakhalin, 26 },
    { (char*)"Asia/Samarkand", XK_PG_Asia_Samarkand, 13 },
    { (char*)"Asia/Seoul", XK_PG_Asia_Seoul, 21 },
    { (char*)"Asia/Shanghai", XK_PG_Asia_Shanghai, 21 },
    { (char*)"Asia/Singapore", XK_PG_Asia_Singapore, 10 },
    { (char*)"Asia/Srednekolymsk", XK_PG_Asia_Srednekolymsk, 24 },
    { (char*)"Asia/Taipei", XK_PG_Asia_Taipei, 20 },
    { (char*)"Asia/Tashkent", XK_PG_Asia_Tashkent, 11 },
    { (char*)"Asia/Tbilisi", XK_PG_Asia_Tbilisi, 20 },
    { (char*)"Asia/Tehran", XK_PG_Asia_Tehran, 107 },
    { (char*)"Asia/Tel_Aviv", XK_PG_Asia_Tel_Aviv, 92 },
    { (char*)"Asia/Thimbu", XK_PG_Asia_Thimbu, 5 },
    { (char*)"Asia/Thimphu", XK_PG_Asia_Thimphu, 4 },
    { (char*)"Asia/Tokyo", XK_PG_Asia_Tokyo, 7 },
    { (char*)"Asia/Tomsk", XK_PG_Asia_Tomsk, 26 },
    { (char*)"Asia/Ujung_Pandang", XK_PG_Asia_Ujung_Pandang, 7 },
    { (char*)"Asia/Ulaanbaatar", XK_PG_Asia_Ulaanbaatar, 13 },
    { (char*)"Asia/Ulan_Bator", XK_PG_Asia_Ulan_Bator, 14 },
    { (char*)"Asia/Urumqi", XK_PG_Asia_Urumqi, 7 },
    { (char*)"Asia/Ust-Nera", XK_PG_Asia_Ust_Nera, 26 },
    { (char*)"Asia/Vientiane", XK_PG_Asia_Vientiane, 5 },
    { (char*)"Asia/Vladivostok", XK_PG_Asia_Vladivostok, 24 },
    { (char*)"Asia/Yakutsk", XK_PG_Asia_Yakutsk, 24 },
    { (char*)"Asia/Yangon", XK_PG_Asia_Yangon, 6 },
    { (char*)"Asia/Yekaterinburg", XK_PG_Asia_Yekaterinburg, 25 },
    { (char*)"Asia/Yerevan", XK_PG_Asia_Yerevan, 15 }
};

static local_tzdata_dispatch_struct atlantic_tzdata_info[] =
{
    { (char*)"Atlantic/Azores", XK_PG_Atlantic_Azores, 70 },
    { (char*)"Atlantic/Bermuda", XK_PG_Atlantic_Bermuda, 28 },
    { (char*)"Atlantic/Canary", XK_PG_Atlantic_Canary, 12 },
    { (char*)"Atlantic/Cape_Verde", XK_PG_Atlantic_Cape_Verde, 6 },
    { (char*)"Atlantic/Faeroe", XK_PG_Atlantic_Faeroe, 11 },
    { (char*)"Atlantic/Faroe", XK_PG_Atlantic_Faroe, 10 },
    { (char*)"Atlantic/Jan_Mayen", XK_PG_Atlantic_Jan_Mayen, 37 },
    { (char*)"Atlantic/Madeira", XK_PG_Atlantic_Madeira, 62 },
    { (char*)"Atlantic/Reykjavik", XK_PG_Atlantic_Reykjavik, 20 },
    { (char*)"Atlantic/South_Georgia", XK_PG_Atlantic_South_Georgia, 3 },
    { (char*)"Atlantic/Stanley", XK_PG_Atlantic_Stanley, 19 },
    { (char*)"Atlantic/St_Helena", XK_PG_Atlantic_St_Helena, 4 }
};

static local_tzdata_dispatch_struct australia_tzdata_info[] =
{
    { (char*)"Australia/ACT", XK_PG_Australia_ACT, 28 },
    { (char*)"Australia/Adelaide", XK_PG_Australia_Adelaide, 27 },
    { (char*)"Australia/Brisbane", XK_PG_Australia_Brisbane, 15 },
    { (char*)"Australia/Broken_Hill", XK_PG_Australia_Broken_Hill, 45 },
    { (char*)"Australia/Canberra", XK_PG_Australia_Canberra, 28 },
    { (char*)"Australia/Currie", XK_PG_Australia_Currie, 32 },
    { (char*)"Australia/Darwin", XK_PG_Australia_Darwin, 11 },
    { (char*)"Australia/Eucla", XK_PG_Australia_Eucla, 20 },
    { (char*)"Australia/Hobart", XK_PG_Australia_Hobart, 32 },
    { (char*)"Australia/LHI", XK_PG_Australia_LHI, 20 },
    { (char*)"Australia/Lindeman", XK_PG_Australia_Lindeman, 18 },
    { (char*)"Australia/Lord_Howe", XK_PG_Australia_Lord_Howe, 19 },
    { (char*)"Australia/Melbourne", XK_PG_Australia_Melbourne, 25 },
    { (char*)"Australia/North", XK_PG_Australia_North, 12 },
    { (char*)"Australia/NSW", XK_PG_Australia_NSW, 28 },
    { (char*)"Australia/Perth", XK_PG_Australia_Perth, 20 },
    { (char*)"Australia/Queensland", XK_PG_Australia_Queensland, 16 },
    { (char*)"Australia/South", XK_PG_Australia_South, 28 },
    { (char*)"Australia/Sydney", XK_PG_Australia_Sydney, 27 },
    { (char*)"Australia/Tasmania", XK_PG_Australia_Tasmania, 33 },
    { (char*)"Australia/Victoria", XK_PG_Australia_Victoria, 26 },
    { (char*)"Australia/West", XK_PG_Australia_West, 21 },
    { (char*)"Australia/Yancowinna", XK_PG_Australia_Yancowinna, 46 }
};

static local_tzdata_dispatch_struct brazil_tzdata_info[] =
{
    { (char*)"Brazil/Acre", XK_PG_Brazil_Acre, 64 },
    { (char*)"Brazil/DeNoronha", XK_PG_Brazil_DeNoronha, 66 },
    { (char*)"Brazil/East", XK_PG_Brazil_East, 63 },
    { (char*)"Brazil/West", XK_PG_Brazil_West, 64 }
};

static local_tzdata_dispatch_struct canada_tzdata_info[] =
{
    { (char*)"Canada/Atlantic", XK_PG_Canada_Atlantic, 60 },
    { (char*)"Canada/Central", XK_PG_Canada_Central, 39 },
    { (char*)"Canada/Eastern", XK_PG_Canada_Eastern, 40 },
    { (char*)"Canada/Mountain", XK_PG_Canada_Mountain, 28 },
    { (char*)"Canada/Newfoundland", XK_PG_Canada_Newfoundland, 39 },
    { (char*)"Canada/Pacific", XK_PG_Canada_Pacific, 24 },
    { (char*)"Canada/Saskatchewan", XK_PG_Canada_Saskatchewan, 22 },
    { (char*)"Canada/Yukon", XK_PG_Canada_Yukon, 29 }
};

static local_tzdata_dispatch_struct chile_tzdata_info[] =
{
    { (char*)"Chile/Continental", XK_PG_Chile_Continental, 44 },
    { (char*)"Chile/EasterIsland", XK_PG_Chile_EasterIsland, 40 }
};

static local_tzdata_dispatch_struct etc_tzdata_info[] =
{
    { (char*)"Etc/GMT", XK_PG_Etc_GMT, 2 },
    { (char*)"Etc/GMT0", XK_PG_Etc_GMT0, 3 },
    { (char*)"Etc/GMT-0", XK_PG_Etc_GMT_dash_0, 3 },
    { (char*)"Etc/GMT+0", XK_PG_Etc_GMT_plus_0, 3 },
    { (char*)"Etc/GMT-1", XK_PG_Etc_GMT_dash_1, 2 },
    { (char*)"Etc/GMT+1", XK_PG_Etc_GMT_plus_1, 2 },
    { (char*)"Etc/GMT-10", XK_PG_Etc_GMT_dash_10, 2 },
    { (char*)"Etc/GMT+10", XK_PG_Etc_GMT_plus_10, 2 },
    { (char*)"Etc/GMT-11", XK_PG_Etc_GMT_dash_11, 2 },
    { (char*)"Etc/GMT+11", XK_PG_Etc_GMT_plus_11, 2 },
    { (char*)"Etc/GMT-12", XK_PG_Etc_GMT_dash_12, 2 },
    { (char*)"Etc/GMT+12", XK_PG_Etc_GMT_plus_12, 2 },
    { (char*)"Etc/GMT-13", XK_PG_Etc_GMT_dash_13, 2 },
    { (char*)"Etc/GMT-14", XK_PG_Etc_GMT_dash_14, 2 },
    { (char*)"Etc/GMT-2", XK_PG_Etc_GMT_dash_2, 2 },
    { (char*)"Etc/GMT+2", XK_PG_Etc_GMT_plus_2, 2 },
    { (char*)"Etc/GMT-3", XK_PG_Etc_GMT_dash_3, 2 },
    { (char*)"Etc/GMT+3", XK_PG_Etc_GMT_plus_3, 2 },
    { (char*)"Etc/GMT-4", XK_PG_Etc_GMT_dash_4, 2 },
    { (char*)"Etc/GMT+4", XK_PG_Etc_GMT_plus_4, 2 },
    { (char*)"Etc/GMT-5", XK_PG_Etc_GMT_dash_5, 2 },
    { (char*)"Etc/GMT+5", XK_PG_Etc_GMT_plus_5, 2 },
    { (char*)"Etc/GMT-6", XK_PG_Etc_GMT_dash_6, 2 },
    { (char*)"Etc/GMT+6", XK_PG_Etc_GMT_plus_6, 2 },
    { (char*)"Etc/GMT-7", XK_PG_Etc_GMT_dash_7, 2 },
    { (char*)"Etc/GMT+7", XK_PG_Etc_GMT_plus_7, 2 },
    { (char*)"Etc/GMT-8", XK_PG_Etc_GMT_dash_8, 2 },
    { (char*)"Etc/GMT+8", XK_PG_Etc_GMT_plus_8, 2 },
    { (char*)"Etc/GMT-9", XK_PG_Etc_GMT_dash_9, 2 },
    { (char*)"Etc/GMT+9", XK_PG_Etc_GMT_plus_9, 2 },
    { (char*)"Etc/Greenwich", XK_PG_Etc_Greenwich, 3 },
    { (char*)"Etc/UCT", XK_PG_Etc_UCT, 3 },
    { (char*)"Etc/Universal", XK_PG_Etc_Universal, 3 },
    { (char*)"Etc/UTC", XK_PG_Etc_UTC, 2 },
    { (char*)"Etc/Zulu", XK_PG_Etc_Zulu, 2 }
};

static local_tzdata_dispatch_struct europe_tzdata_info[] =
{
    { (char*)"Europe/Amsterdam", XK_PG_Europe_Amsterdam, 50 },
    { (char*)"Europe/Andorra", XK_PG_Europe_Andorra, 11 },
    { (char*)"Europe/Astrakhan", XK_PG_Europe_Astrakhan, 26 },
    { (char*)"Europe/Athens", XK_PG_Europe_Athens, 34 },
    { (char*)"Europe/Belfast", XK_PG_Europe_Belfast, 78 },
    { (char*)"Europe/Belgrade", XK_PG_Europe_Belgrade, 31 },
    { (char*)"Europe/Berlin", XK_PG_Europe_Berlin, 40 },
    { (char*)"Europe/Bratislava", XK_PG_Europe_Bratislava, 38 },
    { (char*)"Europe/Brussels", XK_PG_Europe_Brussels, 67 },
    { (char*)"Europe/Bucharest", XK_PG_Europe_Bucharest, 46 },
    { (char*)"Europe/Budapest", XK_PG_Europe_Budapest, 49 },
    { (char*)"Europe/Busingen", XK_PG_Europe_Busingen, 14 },
    { (char*)"Europe/Chisinau", XK_PG_Europe_Chisinau, 61 },
    { (char*)"Europe/Copenhagen", XK_PG_Europe_Copenhagen, 41 },
    { (char*)"Europe/Dublin", XK_PG_Europe_Dublin, 84 },
    { (char*)"Europe/Gibraltar", XK_PG_Europe_Gibraltar, 76 },
    { (char*)"Europe/Guernsey", XK_PG_Europe_Guernsey, 78 },
    { (char*)"Europe/Helsinki", XK_PG_Europe_Helsinki, 15 },
    { (char*)"Europe/Isle_of_Man", XK_PG_Europe_Isle_of_Man, 78 },
    { (char*)"Europe/Istanbul", XK_PG_Europe_Istanbul, 65 },
    { (char*)"Europe/Jersey", XK_PG_Europe_Jersey, 78 },
    { (char*)"Europe/Kaliningrad", XK_PG_Europe_Kaliningrad, 61 },
    { (char*)"Europe/Kiev", XK_PG_Europe_Kiev, 54 },
    { (char*)"Europe/Kirov", XK_PG_Europe_Kirov, 24 },
    { (char*)"Europe/Lisbon", XK_PG_Europe_Lisbon, 71 },
    { (char*)"Europe/Ljubljana", XK_PG_Europe_Ljubljana, 32 },
    { (char*)"Europe/London", XK_PG_Europe_London, 77 },
    { (char*)"Europe/Luxembourg", XK_PG_Europe_Luxembourg, 88 },
    { (char*)"Europe/Madrid", XK_PG_Europe_Madrid, 41 },
    { (char*)"Europe/Malta", XK_PG_Europe_Malta, 59 },
    { (char*)"Europe/Mariehamn", XK_PG_Europe_Mariehamn, 16 },
    { (char*)"Europe/Minsk", XK_PG_Europe_Minsk, 43 },
    { (char*)"Europe/Monaco", XK_PG_Europe_Monaco, 48 },
    { (char*)"Europe/Moscow", XK_PG_Europe_Moscow, 28 },
    { (char*)"Europe/Nicosia", XK_PG_Europe_Nicosia, 17 },
    { (char*)"Europe/Oslo", XK_PG_Europe_Oslo, 36 },
    { (char*)"Europe/Paris", XK_PG_Europe_Paris, 73 },
    { (char*)"Europe/Podgorica", XK_PG_Europe_Podgorica, 32 },
    { (char*)"Europe/Prague", XK_PG_Europe_Prague, 37 },
    { (char*)"Europe/Riga", XK_PG_Europe_Riga, 57 },
    { (char*)"Europe/Rome", XK_PG_Europe_Rome, 71 },
    { (char*)"Europe/Samara", XK_PG_Europe_Samara, 27 },
    { (char*)"Europe/San_Marino", XK_PG_Europe_San_Marino, 72 },
    { (char*)"Europe/Sarajevo", XK_PG_Europe_Sarajevo, 32 },
    { (char*)"Europe/Saratov", XK_PG_Europe_Saratov, 26 },
    { (char*)"Europe/Simferopol", XK_PG_Europe_Simferopol, 62 },
    { (char*)"Europe/Skopje", XK_PG_Europe_Skopje, 32 },
    { (char*)"Europe/Sofia", XK_PG_Europe_Sofia, 45 },
    { (char*)"Europe/Stockholm", XK_PG_Europe_Stockholm, 13 },
    { (char*)"Europe/Tallinn", XK_PG_Europe_Tallinn, 53 },
    { (char*)"Europe/Tirane", XK_PG_Europe_Tirane, 36 },
    { (char*)"Europe/Tiraspol", XK_PG_Europe_Tiraspol, 62 },
    { (char*)"Europe/Uzhgorod", XK_PG_Europe_Uzhgorod, 57 },
    { (char*)"Europe/Vaduz", XK_PG_Europe_Vaduz, 14 },
    { (char*)"Europe/Vatican", XK_PG_Europe_Vatican, 72 },
    { (char*)"Europe/Vienna", XK_PG_Europe_Vienna, 41 },
    { (char*)"Europe/Vilnius", XK_PG_Europe_Vilnius, 55 },
    { (char*)"Europe/Volgograd", XK_PG_Europe_Volgograd, 27 },
    { (char*)"Europe/Warsaw", XK_PG_Europe_Warsaw, 59 },
    { (char*)"Europe/Zagreb", XK_PG_Europe_Zagreb, 32 },
    { (char*)"Europe/Zaporozhye", XK_PG_Europe_Zaporozhye, 54 },
    { (char*)"Europe/Zurich", XK_PG_Europe_Zurich, 13 }
};

static local_tzdata_dispatch_struct indian_tzdata_info[] =
{
    { (char*)"Indian/Antananarivo", XK_PG_Indian_Antananarivo, 7 },
    { (char*)"Indian/Chagos", XK_PG_Indian_Chagos, 4 },
    { (char*)"Indian/Christmas", XK_PG_Indian_Christmas, 3 },
    { (char*)"Indian/Cocos", XK_PG_Indian_Cocos, 3 },
    { (char*)"Indian/Comoro", XK_PG_Indian_Comoro, 7 },
    { (char*)"Indian/Kerguelen", XK_PG_Indian_Kerguelen, 3 },
    { (char*)"Indian/Mahe", XK_PG_Indian_Mahe, 3 },
    { (char*)"Indian/Maldives", XK_PG_Indian_Maldives, 4 },
    { (char*)"Indian/Mauritius", XK_PG_Indian_Mauritius, 7 },
    { (char*)"Indian/Mayotte", XK_PG_Indian_Mayotte, 7 },
    { (char*)"Indian/Reunion", XK_PG_Indian_Reunion, 3 }
};

static local_tzdata_dispatch_struct mexico_tzdata_info[] =
{
    { (char*)"Mexico/BajaNorte", XK_PG_Mexico_BajaNorte, 53 },
    { (char*)"Mexico/BajaSur", XK_PG_Mexico_BajaSur, 26 },
    { (char*)"Mexico/General", XK_PG_Mexico_General, 25 }
};

static local_tzdata_dispatch_struct mideast_tzdata_info[] =
{
    { (char*)"Mideast/Riyadh87", XK_PG_Mideast_Riyadh87, 370 },
    { (char*)"Mideast/Riyadh88", XK_PG_Mideast_Riyadh88, 371 },
    { (char*)"Mideast/Riyadh89", XK_PG_Mideast_Riyadh89, 370 }
};

static local_tzdata_dispatch_struct pacific_tzdata_info[] =
{
    { (char*)"Pacific/Apia", XK_PG_Pacific_Apia, 11 },
    { (char*)"Pacific/Auckland", XK_PG_Pacific_Auckland, 20 },
    { (char*)"Pacific/Chatham", XK_PG_Pacific_Chatham, 12 },
    { (char*)"Pacific/Chuuk", XK_PG_Pacific_Chuuk, 8 },
    { (char*)"Pacific/Easter", XK_PG_Pacific_Easter, 39 },
    { (char*)"Pacific/Efate", XK_PG_Pacific_Efate, 9 },
    { (char*)"Pacific/Enderbury", XK_PG_Pacific_Enderbury, 5 },
    { (char*)"Pacific/Fakaofo", XK_PG_Pacific_Fakaofo, 4 },
    { (char*)"Pacific/Fiji", XK_PG_Pacific_Fiji, 14 },
    { (char*)"Pacific/Funafuti", XK_PG_Pacific_Funafuti, 3 },
    { (char*)"Pacific/Galapagos", XK_PG_Pacific_Galapagos, 6 },
    { (char*)"Pacific/Gambier", XK_PG_Pacific_Gambier, 3 },
    { (char*)"Pacific/Guadalcanal", XK_PG_Pacific_Guadalcanal, 3 },
    { (char*)"Pacific/Guam", XK_PG_Pacific_Guam, 21 },
    { (char*)"Pacific/Honolulu", XK_PG_Pacific_Honolulu, 19 },
    { (char*)"Pacific/Johnston", XK_PG_Pacific_Johnston, 20 },
    { (char*)"Pacific/Kiritimati", XK_PG_Pacific_Kiritimati, 5 },
    { (char*)"Pacific/Kosrae", XK_PG_Pacific_Kosrae, 11 },
    { (char*)"Pacific/Kwajalein", XK_PG_Pacific_Kwajalein, 8 },
    { (char*)"Pacific/Majuro", XK_PG_Pacific_Majuro, 9 },
    { (char*)"Pacific/Marquesas", XK_PG_Pacific_Marquesas, 3 },
    { (char*)"Pacific/Midway", XK_PG_Pacific_Midway, 5 },
    { (char*)"Pacific/Nauru", XK_PG_Pacific_Nauru, 6 },
    { (char*)"Pacific/Niue", XK_PG_Pacific_Niue, 5 },
    { (char*)"Pacific/Norfolk", XK_PG_Pacific_Norfolk, 24 },
    { (char*)"Pacific/Noumea", XK_PG_Pacific_Noumea, 7 },
    { (char*)"Pacific/Pago_Pago", XK_PG_Pacific_Pago_Pago, 4 },
    { (char*)"Pacific/Palau", XK_PG_Pacific_Palau, 4 },
    { (char*)"Pacific/Pitcairn", XK_PG_Pacific_Pitcairn, 4 },
    { (char*)"Pacific/Pohnpei", XK_PG_Pacific_Pohnpei, 9 },
    { (char*)"Pacific/Ponape", XK_PG_Pacific_Ponape, 10 },
    { (char*)"Pacific/Port_Moresby", XK_PG_Pacific_Port_Moresby, 4 },
    { (char*)"Pacific/Rarotonga", XK_PG_Pacific_Rarotonga, 7 },
    { (char*)"Pacific/Saipan", XK_PG_Pacific_Saipan, 22 },
    { (char*)"Pacific/Samoa", XK_PG_Pacific_Samoa, 5 },
    { (char*)"Pacific/Tahiti", XK_PG_Pacific_Tahiti, 3 },
    { (char*)"Pacific/Tarawa", XK_PG_Pacific_Tarawa, 3 },
    { (char*)"Pacific/Tongatapu", XK_PG_Pacific_Tongatapu, 11 },
    { (char*)"Pacific/Truk", XK_PG_Pacific_Truk, 9 },
    { (char*)"Pacific/Wake", XK_PG_Pacific_Wake, 3 },
    { (char*)"Pacific/Wallis", XK_PG_Pacific_Wallis, 3 },
    { (char*)"Pacific/Yap", XK_PG_Pacific_Yap, 9 }
};

static local_tzdata_dispatch_struct us_tzdata_info[] =
{
    { (char*)"US/Alaska", XK_PG_US_Alaska, 23 },
    { (char*)"US/Aleutian", XK_PG_US_Aleutian, 24 },
    { (char*)"US/Arizona", XK_PG_US_Arizona, 22 },
    { (char*)"US/Central", XK_PG_US_Central, 29 },
    { (char*)"US/Eastern", XK_PG_US_Eastern, 26 },
    { (char*)"US/East-Indiana", XK_PG_US_East_Indiana, 29 },
    { (char*)"US/Hawaii", XK_PG_US_Hawaii, 20 },
    { (char*)"US/Indiana-Starke", XK_PG_US_Indiana_Starke, 27 },
    { (char*)"US/Michigan", XK_PG_US_Michigan, 27 },
    { (char*)"US/Mountain", XK_PG_US_Mountain, 26 },
    { (char*)"US/Pacific", XK_PG_US_Pacific, 24 },
    { (char*)"US/Pacific-New", XK_PG_US_Pacific_New, 24 },
    { (char*)"US/Samoa", XK_PG_US_Samoa, 5 }
};

static local_tzdata_dispatch_struct top_tzdata_info[] =
{
    { (char*)"CET", XK_PG_CET, 19 },
    { (char*)"CST6CDT", XK_PG_CST6CDT, 15 },
    { (char*)"Cuba", XK_PG_Cuba, 44 },
    { (char*)"EET", XK_PG_EET, 8 },
    { (char*)"Egypt", XK_PG_Egypt, 36 },
    { (char*)"Eire", XK_PG_Eire, 85 },
    { (char*)"EST", XK_PG_EST, 2 },
    { (char*)"EST5EDT", XK_PG_EST5EDT, 15 },
    { (char*)"Factory", XK_PG_Factory, 2 },
    { (char*)"GB", XK_PG_GB, 78 },
    { (char*)"GB-Eire", XK_PG_GB_Eire, 78 },
    { (char*)"GMT", XK_PG_GMT, 3 },
    { (char*)"GMT0", XK_PG_GMT0, 3 },
    { (char*)"GMT-0", XK_PG_GMT_dash_0, 3 },
    { (char*)"GMT+0", XK_PG_GMT_plus_0, 3 },
    { (char*)"Greenwich", XK_PG_Greenwich, 3 },
    { (char*)"Hongkong", XK_PG_Hongkong, 22 },
    { (char*)"HST", XK_PG_HST, 2 },
    { (char*)"Iceland", XK_PG_Iceland, 21 },
    { (char*)"Iran", XK_PG_Iran, 108 },
    { (char*)"Israel", XK_PG_Israel, 92 },
    { (char*)"Jamaica", XK_PG_Jamaica, 20 },
    { (char*)"Japan", XK_PG_Japan, 8 },
    { (char*)"Kwajalein", XK_PG_Kwajalein, 9 },
    { (char*)"Libya", XK_PG_Libya, 28 },
    { (char*)"MET", XK_PG_MET, 19 },
    { (char*)"MST", XK_PG_MST, 2 },
    { (char*)"MST7MDT", XK_PG_MST7MDT, 15 },
    { (char*)"Navajo", XK_PG_Navajo, 26 },
    { (char*)"NZ", XK_PG_NZ, 21 },
    { (char*)"NZ-CHAT", XK_PG_NZ_CHAT, 13 },
    { (char*)"Poland", XK_PG_Poland, 60 },
    { (char*)"Portugal", XK_PG_Portugal, 72 },
    { (char*)"posixrules", XK_PG_posixrules, 1 },
    { (char*)"PRC", XK_PG_PRC, 22 },
    { (char*)"PST8PDT", XK_PG_PST8PDT, 15 },
    { (char*)"ROC", XK_PG_ROC, 21 },
    { (char*)"ROK", XK_PG_ROK, 22 },
    { (char*)"Singapore", XK_PG_Singapore, 11 },
    { (char*)"Turkey", XK_PG_Turkey, 66 },
    { (char*)"UCT", XK_PG_UCT, 3 },
    { (char*)"Universal", XK_PG_Universal, 3 },
    { (char*)"UTC", XK_PG_UTC, 3 },
    { (char*)"WET", XK_PG_WET, 8 },
    { (char*)"W-SU", XK_PG_W_SU, 29 },
    { (char*)"Zulu", XK_PG_Zulu, 3 }
};

char** xk_pg_parser_get_tzdata_info(const char* tz_name, int32_t* tz_dataSize)
{
    local_tzdata_dispatch_struct*       entry = NULL;
    size_t                              i = 0,
                                        entrySize = 0;

    if (0 == strncmp(tz_name, "Africa", 6))
    {
        entry = africa_tzdata_info;
        entrySize = sizeof(africa_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "America", 7))
    {
        entry = america_tzdata_info;
        entrySize = sizeof(america_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Antarctica", 10))
    {
        entry = antartica_tzdata_info;
        entrySize = sizeof(antartica_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Arctic", 6))
    {
        entry = arctic_tzdata_info;
        entrySize = sizeof(arctic_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Asia", 4))
    {
        entry = asia_tzdata_info;
        entrySize = sizeof(asia_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Atlantic", 8))
    {
        entry = atlantic_tzdata_info;
        entrySize = sizeof(atlantic_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Australia", 9))
    {
        entry = australia_tzdata_info;
        entrySize = sizeof(australia_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Brazil", 6))
    {
        entry = brazil_tzdata_info;
        entrySize = sizeof(brazil_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Canada", 6))
    {
        entry = canada_tzdata_info;
        entrySize = sizeof(canada_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Chile", 5))
    {
        entry = chile_tzdata_info;
        entrySize = sizeof(chile_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Etc", 3))
    {
        entry = etc_tzdata_info;
        entrySize = sizeof(etc_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Europe", 6))
    {
        entry = europe_tzdata_info;
        entrySize = sizeof(europe_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Indian", 6))
    {
        entry = indian_tzdata_info;
        entrySize = sizeof(indian_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Mexico", 6))
    {
        entry = mexico_tzdata_info;
        entrySize = sizeof(mexico_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Mideast", 7))
    {
        entry = mideast_tzdata_info;
        entrySize = sizeof(mideast_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "Pacific", 7))
    {
        entry = pacific_tzdata_info;
        entrySize = sizeof(pacific_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else if (0 == strncmp(tz_name, "US", 2))
    {
        entry = us_tzdata_info;
        entrySize = sizeof(us_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }
    else
    {
        entry = top_tzdata_info;
        entrySize = sizeof(top_tzdata_info) / sizeof(local_tzdata_dispatch_struct);
    }

    for (i = 0; i < entrySize; ++i)
    {
        if (0 == strcmp(tz_name, entry[i].tz_name))
        {
            *tz_dataSize = entry[i].tz_data_size;
            return entry[i].tz_data;
        }
    }

    return NULL;
}
