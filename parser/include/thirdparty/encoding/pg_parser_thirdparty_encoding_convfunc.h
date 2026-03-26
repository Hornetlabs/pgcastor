#ifndef pg_parser_thirdparty_encoding_CONVFUNC_H
#define pg_parser_thirdparty_encoding_CONVFUNC_H
/*
 * Charset conversion functions:
 *    src_str - string to be converted;
 *    dest_str - converted string;
 *    str_len - length of src_str;
 */
extern void ascii_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_ascii(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void koi8r_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_koi8r(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void iso_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_iso(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void win1251_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_win1251(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void win866_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_win866(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void koi8r_to_win1251(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void win1251_to_koi8r(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void koi8r_to_win866(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void win866_to_koi8r(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void win866_to_win1251(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void win1251_to_win866(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void iso_to_koi8r(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void koi8r_to_iso(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void iso_to_win1251(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void win1251_to_iso(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void iso_to_win866(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void win866_to_iso(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void euc_cn_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_euc_cn(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void euc_jp_to_sjis(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void sjis_to_euc_jp(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void euc_jp_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void sjis_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_euc_jp(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_sjis(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void euc_kr_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_euc_kr(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void euc_tw_to_big5(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void big5_to_euc_tw(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void euc_tw_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void big5_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_euc_tw(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_big5(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void latin2_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_latin2(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void win1250_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_win1250(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void latin2_to_win1250(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void win1250_to_latin2(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void latin1_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_latin1(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void latin3_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_latin3(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void latin4_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void mic_to_latin4(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void ascii_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_ascii(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void big5_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_big5(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_koi8r(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void koi8r_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_koi8u(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void koi8u_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_win(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void win_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void euc_cn_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_euc_cn(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void euc_jp_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_euc_jp(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void euc_kr_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_euc_kr(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void euc_tw_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_euc_tw(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void gb18030_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_gb18030(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void gbk_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_gbk(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_iso8859(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void iso8859_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void iso8859_1_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_iso8859_1(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void johab_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_johab(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void sjis_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_sjis(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void uhc_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_uhc(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void euc_jis_2004_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void utf8_to_euc_jis_2004(unsigned char* src_str, unsigned char* dest_str, int32_t str_len);
extern void shift_jis_2004_to_utf8(unsigned char* src_str,
                                   unsigned char* dest_str,
                                   int32_t        str_len);
extern void utf8_to_shift_jis_2004(unsigned char* src_str,
                                   unsigned char* dest_str,
                                   int32_t        str_len);
extern void euc_jis_2004_to_shift_jis_2004(unsigned char* src_str,
                                           unsigned char* dest_str,
                                           int32_t        str_len);
extern void shift_jis_2004_to_euc_jis_2004(unsigned char* src_str,
                                           unsigned char* dest_str,
                                           int32_t        str_len);

#endif
