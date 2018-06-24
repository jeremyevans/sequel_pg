#define SEQUEL_PG_VERSION_INTEGER 10900

#include <string.h>
#include <stdio.h>
#include <math.h>
#include <libpq-fe.h>
#include <ruby.h>
#include <ctype.h>
#include <sys/types.h>
#include <time.h>

#include <ruby/version.h>
#include <ruby/encoding.h>

#ifndef SPG_MAX_FIELDS
#define SPG_MAX_FIELDS 256
#endif
#define SPG_MINUTES_PER_DAY 1440.0
#define SPG_SECONDS_PER_DAY 86400.0

#define SPG_DT_ADD_USEC if (usec != 0) { dt = rb_funcall(dt, spg_id_op_plus, 1, rb_Rational2(INT2NUM(usec), spg_usec_per_day)); }

#define SPG_NO_TZ          (0)
#define SPG_DB_LOCAL       (1)
#define SPG_DB_UTC         (1<<1)
#define SPG_DB_CUSTOM      (1 + (1<<1))
#define SPG_APP_LOCAL      (1<<2)
#define SPG_APP_UTC        (1<<3)
#define SPG_APP_CUSTOM     ((1<<2) + (1<<3))
#define SPG_TZ_INITIALIZED (1<<4)
#define SPG_USE_TIME       (1<<5)
#define SPG_HAS_TIMEZONE   (1<<6)

#define SPG_YEAR_SHIFT  16
#define SPG_MONTH_SHIFT 8
#define SPG_MONTH_MASK  0x0000ffff
#define SPG_DAY_MASK    0x0000001f
#define SPG_TIME_UTC    32

#define SPG_YIELD_NORMAL 0
#define SPG_YIELD_COLUMN 1
#define SPG_YIELD_COLUMNS 2
#define SPG_YIELD_FIRST 3
#define SPG_YIELD_ARRAY 4
#define SPG_YIELD_KV_HASH 5
#define SPG_YIELD_MKV_HASH 6
#define SPG_YIELD_KMV_HASH 7
#define SPG_YIELD_MKMV_HASH 8
#define SPG_YIELD_MODEL 9
#define SPG_YIELD_KV_HASH_GROUPS 10
#define SPG_YIELD_MKV_HASH_GROUPS 11
#define SPG_YIELD_KMV_HASH_GROUPS 12
#define SPG_YIELD_MKMV_HASH_GROUPS 13

/* External functions defined by ruby-pg when data objects are structs */
PGconn* pg_get_pgconn(VALUE);
PGresult* pgresult_get(VALUE);

static VALUE spg_Sequel;
static VALUE spg_PGArray;
static VALUE spg_Blob;
static VALUE spg_Blob_instance;
static VALUE spg_Kernel;
static VALUE spg_Date;
static VALUE spg_DateTime;
static VALUE spg_SQLTime;
static VALUE spg_PGError;

static VALUE spg_sym_utc;
static VALUE spg_sym_local;
static VALUE spg_sym_map;
static VALUE spg_sym_first;
static VALUE spg_sym_array;
static VALUE spg_sym_hash;
static VALUE spg_sym_hash_groups;
static VALUE spg_sym_model;
static VALUE spg_sym__sequel_pg_type;
static VALUE spg_sym__sequel_pg_value;

static VALUE spg_sym_text;
static VALUE spg_sym_character_varying;
static VALUE spg_sym_integer;
static VALUE spg_sym_timestamp;
static VALUE spg_sym_timestamptz;
static VALUE spg_sym_time;
static VALUE spg_sym_timetz;
static VALUE spg_sym_bigint;
static VALUE spg_sym_numeric;
static VALUE spg_sym_double_precision;
static VALUE spg_sym_boolean;
static VALUE spg_sym_bytea;
static VALUE spg_sym_date;
static VALUE spg_sym_smallint;
static VALUE spg_sym_oid;
static VALUE spg_sym_real;
static VALUE spg_sym_xml;
static VALUE spg_sym_money;
static VALUE spg_sym_bit;
static VALUE spg_sym_bit_varying;
static VALUE spg_sym_uuid;
static VALUE spg_sym_xid;
static VALUE spg_sym_cid;
static VALUE spg_sym_name;
static VALUE spg_sym_tid;
static VALUE spg_sym_int2vector;

static VALUE spg_nan;
static VALUE spg_pos_inf;
static VALUE spg_neg_inf;
static VALUE spg_usec_per_day;

static ID spg_id_BigDecimal;
static ID spg_id_new;
static ID spg_id_date;
static ID spg_id_local;
static ID spg_id_year;
static ID spg_id_month;
static ID spg_id_day;
static ID spg_id_output_identifier;
static ID spg_id_datetime_class;
static ID spg_id_application_timezone;
static ID spg_id_to_application_timestamp;
static ID spg_id_timezone;
static ID spg_id_op_plus;
static ID spg_id_utc;
static ID spg_id_utc_offset;
static ID spg_id_localtime;
static ID spg_id_new_offset;
static ID spg_id_convert_infinite_timestamps;
static ID spg_id_infinite_timestamp_value;

static ID spg_id_call;
static ID spg_id_get;
static ID spg_id_opts;

static ID spg_id_db;
static ID spg_id_conversion_procs;

static ID spg_id_columns_equal;
static ID spg_id_columns;
static ID spg_id_encoding;
static ID spg_id_values;

#if HAVE_PQSETSINGLEROWMODE
static ID spg_id_get_result;
static ID spg_id_clear;
static ID spg_id_check;
#endif

struct spg_blob_initialization {
  char *blob_string;
  size_t length;
};

static int enc_get_index(VALUE val) {
  int i = ENCODING_GET_INLINED(val);
  if (i == ENCODING_INLINE_MAX) {
    i = NUM2INT(rb_ivar_get(val, spg_id_encoding));
  }
  return i;
}

#define PG_ENCODING_SET_NOCHECK(obj,i) \
  do { \
    if ((i) < ENCODING_INLINE_MAX) \
      ENCODING_SET_INLINED((obj), (i)); \
    else \
      rb_enc_set_index((obj), (i)); \
  } while(0)

static VALUE
pg_text_dec_integer(char *val, int len)
{
  long i;
  int max_len;

  if( sizeof(i) >= 8 && FIXNUM_MAX >= 1000000000000000000LL ){
    /* 64 bit system can safely handle all numbers up to 18 digits as Fixnum */
    max_len = 18;
  } else if( sizeof(i) >= 4 && FIXNUM_MAX >= 1000000000LL ){
    /* 32 bit system can safely handle all numbers up to 9 digits as Fixnum */
    max_len = 9;
  } else {
    /* unknown -> don't use fast path for int conversion */
    max_len = 0;
  }

  if( len <= max_len ){
    /* rb_cstr2inum() seems to be slow, so we do the int conversion by hand.
     * This proved to be 40% faster by the following benchmark:
     *
     *   conn.type_mapping_for_results = PG::BasicTypeMapForResults.new conn
     *   Benchmark.measure do
     *     conn.exec("select generate_series(1,1000000)").values }
     *   end
     */
    char *val_pos = val;
    char digit = *val_pos;
    int neg;
    int error = 0;

    if( digit=='-' ){
      neg = 1;
      i = 0;
    }else if( digit>='0' && digit<='9' ){
      neg = 0;
      i = digit - '0';
    } else {
      error = 1;
    }

    while (!error && (digit=*++val_pos)) {
      if( digit>='0' && digit<='9' ){
        i = i * 10 + (digit - '0');
      } else {
        error = 1;
      }
    }

    if( !error ){
      return LONG2FIX(neg ? -i : i);
    }
  }
  /* Fallback to ruby method if number too big or unrecognized. */
  return rb_cstr2inum(val, 10);
}

static VALUE spg__array_col_value(char *v, size_t length, VALUE converter, int enc_index, int oid, VALUE db);

static VALUE read_array(int *index, char *c_pg_array_string, int array_string_length, VALUE buf, VALUE converter, int enc_index, int oid, VALUE db) {
  int word_index = 0;
  char *word = RSTRING_PTR(buf);

  /* The current character in the input string. */
  char c;

  /*  0: Currently outside a quoted string, current word never quoted
   *  1: Currently inside a quoted string
   * -1: Currently outside a quoted string, current word previously quoted */
  int openQuote = 0;

  /* Inside quoted input means the next character should be treated literally,
   * instead of being treated as a metacharacter.
   * Outside of quoted input, means that the word shouldn't be pushed to the array,
   * used when the last entry was a subarray (which adds to the array itself). */
  int escapeNext = 0;

  VALUE array = rb_ary_new();
  RB_GC_GUARD(array);

  /* Special case the empty array, so it doesn't need to be handled manually inside
   * the loop. */
  if(((*index) < array_string_length) && c_pg_array_string[(*index)] == '}') 
  {
    return array;
  }

  for(;(*index) < array_string_length; ++(*index))
  {
    c = c_pg_array_string[*index];
    if(openQuote < 1)
    {
      if(c == ',' || c == '}')
      {
        if(!escapeNext)
        {
          if(openQuote == 0 && word_index == 4 && !strncmp(word, "NULL", word_index))
          {
            rb_ary_push(array, Qnil);
          }
          else 
          {
            word[word_index] = '\0';
            rb_ary_push(array, spg__array_col_value(word, word_index, converter, enc_index, oid, db));
          }
        }
        if(c == '}')
        {
          return array;
        }
        escapeNext = 0;
        openQuote = 0;
        word_index = 0;
      }
      else if(c == '"')
      {
        openQuote = 1;
      }
      else if(c == '{')
      {
        (*index)++;
        rb_ary_push(array, read_array(index, c_pg_array_string, array_string_length, buf, converter, enc_index, oid, db));
        escapeNext = 1;
      }
      else
      {
        word[word_index] = c;
        word_index++;
      }
    }
    else if (escapeNext) {
      word[word_index] = c;
      word_index++;
      escapeNext = 0;
    }
    else if (c == '\\')
    {
      escapeNext = 1;
    }
    else if (c == '"')
    {
      openQuote = -1;
    }
    else
    {
      word[word_index] = c;
      word_index++;
    }
  }

  RB_GC_GUARD(buf);

  return array;
}

static VALUE check_pg_array(int* index, char *c_pg_array_string, int array_string_length) {
  if (array_string_length == 0) {
    rb_raise(rb_eArgError, "unexpected PostgreSQL array format, empty");
  } else if (array_string_length == 2 && c_pg_array_string[0] == '{' && c_pg_array_string[0] == '}') {
    return rb_ary_new();
  }

  switch (c_pg_array_string[0]) {
    case '[':
      /* Skip explicit subscripts, scanning until opening array */
      for(;(*index) < array_string_length && c_pg_array_string[(*index)] != '{'; ++(*index))
        /* nothing */;

      if ((*index) >= array_string_length) {
        rb_raise(rb_eArgError, "unexpected PostgreSQL array format, no {");
      } else {
        ++(*index);
      }
    case '{':
      break;
    default:
      rb_raise(rb_eArgError, "unexpected PostgreSQL array format, doesn't start with { or [");
  }

  return Qnil;
}

static VALUE parse_pg_array(VALUE self, VALUE pg_array_string, VALUE converter) {
  /* convert to c-string, create additional ruby string buffer of
   * the same length, as that will be the worst case. */
  char *c_pg_array_string = StringValueCStr(pg_array_string);
  int array_string_length = RSTRING_LEN(pg_array_string);
  int index = 1;
  VALUE ary;

  if(RTEST(ary = check_pg_array(&index, c_pg_array_string, array_string_length))) {
    return ary;
  }

  return read_array(&index,
    c_pg_array_string,
    array_string_length,
    rb_str_buf_new(array_string_length),
    converter,
    enc_get_index(pg_array_string),
    0,
    Qnil);
}

static VALUE spg_timestamp_error(const char *s, VALUE self, const char *error_msg) {
  self = rb_funcall(self, spg_id_db, 0);
  if(RTEST(rb_funcall(self, spg_id_convert_infinite_timestamps, 0))) {
    if((strcmp(s, "infinity") == 0) || (strcmp(s, "-infinity") == 0)) {
      return rb_funcall(self, spg_id_infinite_timestamp_value, 1, rb_tainted_str_new2(s));
    }
  }
  rb_raise(rb_eArgError, "%s", error_msg);
}

static inline int char_to_digit(char c)
{
  return c - '0';
}

static int str4_to_int(const char *str)
{
  return char_to_digit(str[0]) * 1000
      + char_to_digit(str[1]) * 100
      + char_to_digit(str[2]) * 10
      + char_to_digit(str[3]);
}

static int str2_to_int(const char *str)
{
  return char_to_digit(str[0]) * 10
      + char_to_digit(str[1]);
}

static VALUE spg_time(const char *p, size_t length, int current) {
  int hour, minute, second, i;
  int usec = 0;
  ID meth = spg_id_local;

  if (length < 8) {
    rb_raise(rb_eArgError, "unexpected time format, too short");
  }

  if (p[2] == ':' && p[5] == ':') {
    hour = str2_to_int(p);
    minute = str2_to_int(p+3);
    second = str2_to_int(p+6);
    p += 8;

    if (length >= 10 && p[0] == '.') {
      static const int coef[6] = { 100000, 10000, 1000, 100, 10, 1 };

      p++;
      for (i = 0; i < 6 && isdigit(*p); i++) {
        usec += coef[i] * char_to_digit(*p++);
      }
    }
  } else {
    rb_raise(rb_eArgError, "unexpected time format");
  }

  if (current & SPG_TIME_UTC) {
    meth = spg_id_utc;
  }
  return rb_funcall(spg_SQLTime, meth, 7,
    INT2NUM(current >> SPG_YEAR_SHIFT),
    INT2NUM((current & SPG_MONTH_MASK) >> SPG_MONTH_SHIFT),
    INT2NUM(current & SPG_DAY_MASK),
    INT2NUM(hour),
    INT2NUM(minute),
    INT2NUM(second),
    INT2NUM(usec));
}

/* Caller should check length is at least 4 */
static int parse_year(const char **str, size_t *length) {
  int year;
  size_t remaining = *length;
  const char * p = *str;

  year = str4_to_int(p);
  p += 4;
  remaining -= 4;

  for(int i = 0; isdigit(*p) && i < 3; i++, p++, remaining--) {
    year = 10 * year + char_to_digit(*p);
  }

  *str = p;
  *length = remaining;
  return year;
}

static VALUE spg_date(const char *s, VALUE self, size_t length) {
  int year, month, day;
  const char *p = s;

  if (length < 10) {
    return spg_timestamp_error(s, self, "unexpected date format, too short");
  }

  year = parse_year(&p, &length);

  if (length >= 5 && p[0] == '-' && p[3] == '-') {
    month = str2_to_int(p+1);
    day = str2_to_int(p+4);
  } else {
    return spg_timestamp_error(s, self, "unexpected date format");
  }

  if(s[10] == ' ' && s[11] == 'B' && s[12] == 'C') {
    year = -year;
    year++;
  }

  return rb_funcall(spg_Date, spg_id_new, 3, INT2NUM(year), INT2NUM(month), INT2NUM(day));
}

static VALUE spg_timestamp(const char *s, VALUE self, size_t length, int tz) {
  VALUE dt;
  int year, month, day, hour, min, sec, utc_offset;
  char offset_sign = 0;
  int offset_seconds = 0;
  int usec = 0;
  int i;
  const char *p = s;
  size_t remaining = length;

  if (tz & SPG_DB_CUSTOM || tz & SPG_APP_CUSTOM) {
    return rb_funcall(rb_funcall(self, spg_id_db, 0), spg_id_to_application_timestamp, 1, rb_str_new2(s)); 
  }

  if (remaining < 19) {
    return spg_timestamp_error(s, self, "unexpected timetamp format, too short");
  }

  year = parse_year(&p, &remaining);

  if (remaining >= 15 && p[0] == '-' && p[3] == '-' && p[6] == ' ' && p[9] == ':' && p[12] == ':') {
    month = str2_to_int(p+1);
    day = str2_to_int(p+4);
    hour = str2_to_int(p+7);
    min = str2_to_int(p+10);
    sec = str2_to_int(p+13);
    p += 15;
    remaining -= 15;

    if (remaining >= 2 && p[0] == '.') {
      /* microsecond part, up to 6 digits */
      static const int coef[6] = { 100000, 10000, 1000, 100, 10, 1 };

      p++;
      remaining--;
      for (i = 0; i < 6 && isdigit(*p); i++)
      {
        usec += coef[i] * char_to_digit(*p++);
        remaining--;
      }
    }

    if ((tz & SPG_HAS_TIMEZONE) && remaining >= 3 && (p[0] == '+' || p[0] == '-')) {
      offset_sign = p[0];
      offset_seconds += str2_to_int(p+1)*3600;
      p += 3;
      remaining -= 3;

      if (p[0] == ':') {
        p++;
        remaining--;
      }
      if (remaining >= 2 && isdigit(p[0]) && isdigit(p[1]))
      {
        offset_seconds += str2_to_int(p)*60;
        p += 2;
        remaining -= 2;
      }
      if (p[0] == ':')
      {
        p++;
        remaining--;
      }
      if (remaining >= 2 && isdigit(p[0]) && isdigit(p[1]))
      {
        offset_seconds += str2_to_int(p);
        p += 2;
        remaining -= 2;
      }
      if (offset_sign == '-') {
        offset_seconds *= -1;
      }
    }

    if (remaining == 3 && p[0] == ' ' && p[1] == 'B' && p[2] == 'C') {
      year = -year;
      year++;
    } else if (remaining != 0) {
      return spg_timestamp_error(s, self, "unexpected timestamp format, remaining data left");
    }
  } else {
    return spg_timestamp_error(s, self, "unexpected timestamp format");
  }
  

  if (tz & SPG_USE_TIME) {
#if (RUBY_API_VERSION_MAJOR > 2 || (RUBY_API_VERSION_MAJOR == 2 && RUBY_API_VERSION_MINOR >= 3)) && defined(HAVE_TIMEGM)
    /* Fast path for time conversion */
    struct tm tm;
    struct timespec ts;
    time_t time;

    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    tm.tm_hour = hour;
    tm.tm_min = min;
    tm.tm_sec = sec;
    tm.tm_isdst = -1;

    ts.tv_nsec = usec*1000;

    if (offset_sign) {
      time = timegm(&tm);
      if (time != -1) {
        ts.tv_sec = time - offset_seconds;
        dt = rb_time_timespec_new(&ts, offset_seconds);

        if (tz & SPG_APP_UTC) {
          dt = rb_funcall(dt, spg_id_utc, 0);
        } else if (tz & SPG_APP_LOCAL) {
          dt = rb_funcall(dt, spg_id_local, 0);
        } 

        return dt;
      }
    } else {
      time = mktime(&tm);
      if (time != -1) {
        ts.tv_sec = time;
        if (tz & SPG_DB_UTC) {
          offset_seconds = INT_MAX - 1;
        } else {
          offset_seconds = INT_MAX;
        }

        dt = rb_time_timespec_new(&ts, offset_seconds);

        if (tz & SPG_DB_LOCAL && tz & SPG_APP_UTC) {
          dt = rb_funcall(dt, spg_id_utc, 0);
        } else if (tz & SPG_DB_UTC && tz & SPG_APP_LOCAL) {
          dt = rb_funcall(dt, spg_id_local, 0);
        } 

        return dt;
      }
    }
#endif

    if (offset_sign) {
      /* Offset given, convert to local time if not already in local time.
       * While PostgreSQL generally returns timestamps in local time, it's unwise to rely on this.
       */
      dt = rb_funcall(rb_cTime, spg_id_local, 7, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec), INT2NUM(usec));
      utc_offset = NUM2INT(rb_funcall(dt, spg_id_utc_offset, 0));
      if (utc_offset != offset_seconds) {
        dt = rb_funcall(dt, spg_id_op_plus, 1, INT2NUM(utc_offset - offset_seconds));
      }

      if (tz & SPG_APP_UTC) {
        dt = rb_funcall(dt, spg_id_utc, 0);
      } 
      return dt;
    } else if (tz == SPG_NO_TZ) {
      return rb_funcall(rb_cTime, spg_id_local, 7, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec), INT2NUM(usec));
    }

    /* No offset given, and some timezone combination given */
    if (tz & SPG_DB_UTC) {
      dt = rb_funcall(rb_cTime, spg_id_utc, 7, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec), INT2NUM(usec));
      if (tz & SPG_APP_LOCAL) {
        return rb_funcall(dt, spg_id_localtime, 0);
      } else {
        return dt;
      }
    } else {
      dt = rb_funcall(rb_cTime, spg_id_local, 7, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec), INT2NUM(usec));
      if (tz & SPG_APP_UTC) {
        return rb_funcall(dt, spg_id_utc, 0);
      } else {
        return dt;
      }
    }
  } else {
    /* datetime.class == DateTime */
    double offset_fraction;

    if (offset_sign) {
      /* Offset given, handle correct local time.
       * While PostgreSQL generally returns timestamps in local time, it's unwise to rely on this.
       */
      offset_fraction = offset_seconds/(double)SPG_SECONDS_PER_DAY;
      dt = rb_funcall(spg_DateTime, spg_id_new, 7, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec), rb_float_new(offset_fraction));
      SPG_DT_ADD_USEC

      if (tz & SPG_APP_LOCAL) {
        utc_offset = NUM2INT(rb_funcall(rb_funcall(rb_cTime, spg_id_new, 0), spg_id_utc_offset, 0))/SPG_SECONDS_PER_DAY;
        dt = rb_funcall(dt, spg_id_new_offset, 1, rb_float_new(utc_offset));
      } else if (tz & SPG_APP_UTC) {
        dt = rb_funcall(dt, spg_id_new_offset, 1, INT2NUM(0));
      } 
      return dt;
    } else if (tz == SPG_NO_TZ) {
      dt = rb_funcall(spg_DateTime, spg_id_new, 6, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec));
      SPG_DT_ADD_USEC
      return dt;
    }

    /* No offset given, and some timezone combination given */
    if (tz & SPG_DB_LOCAL) {
      offset_fraction = NUM2INT(rb_funcall(rb_funcall(rb_cTime, spg_id_local, 6, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec)), spg_id_utc_offset, 0))/SPG_SECONDS_PER_DAY;
      dt = rb_funcall(spg_DateTime, spg_id_new, 7, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec), rb_float_new(offset_fraction));
      SPG_DT_ADD_USEC
      if (tz & SPG_APP_UTC) {
        return rb_funcall(dt, spg_id_new_offset, 1, INT2NUM(0));
      } else {
        return dt;
      }
    } else {
      dt = rb_funcall(spg_DateTime, spg_id_new, 6, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec));
      SPG_DT_ADD_USEC
      if (tz & SPG_APP_LOCAL) {
        offset_fraction = NUM2INT(rb_funcall(rb_funcall(rb_cTime, spg_id_local, 6, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec)), spg_id_utc_offset, 0))/SPG_SECONDS_PER_DAY;
        return rb_funcall(dt, spg_id_new_offset, 1, rb_float_new(offset_fraction));
      } else {
        return dt;
      }
    }
  }
}

static VALUE spg_create_Blob(VALUE v) {
  struct spg_blob_initialization *bi = (struct spg_blob_initialization *)v;
  if (bi->blob_string == NULL) {
    rb_raise(rb_eNoMemError, "PQunescapeBytea failure: probably not enough memory");
  }
  return rb_obj_taint(rb_str_new_with_class(spg_Blob_instance, bi->blob_string, bi->length));
}

static VALUE spg_fetch_rows_set_cols(VALUE self, VALUE ignore) {
  return Qnil;
}

static VALUE spg__array_col_value(char *v, size_t length, VALUE converter, int enc_index, int oid, VALUE db) {
  VALUE rv;
  struct spg_blob_initialization bi;

  switch(oid) {
    case 16: /* boolean */
      rv = *v == 't' ? Qtrue : Qfalse;
      break;
    case 17: /* bytea */
      bi.blob_string = (char *)PQunescapeBytea((unsigned char*)v, &bi.length);
      rv = rb_ensure(spg_create_Blob, (VALUE)&bi, (VALUE(*)())PQfreemem, (VALUE)bi.blob_string);
      break;
    case 20: /* integer */
    case 21:
    case 23:
    case 26:
      rv = pg_text_dec_integer(v, length);
      break;
    case 700: /* float */
    case 701:
      if (strcmp("NaN", v) == 0) {
        rv = spg_nan;
      } else if (strcmp("Infinity", v) == 0) {
        rv = spg_pos_inf;
      } else if (strcmp("-Infinity", v) == 0) {
        rv = spg_neg_inf;
      } else {
        rv = rb_float_new(rb_cstr_to_dbl(v, Qfalse));
      }
      break;
    case 1700: /* numeric */
      rv = rb_funcall(spg_Kernel, spg_id_BigDecimal, 1, rb_str_new(v, length));
      break;
    case 1082: /* date */
      rv = spg_date(v, db, length);
      break;
    case 1083: /* time */
    case 1266:
      rv = spg_time(v, length, (int)converter);
      break;
    case 1114: /* timestamp */
    case 1184:
      rv = spg_timestamp(v, db, length, (int)converter);
      break;
    case 18: /* char */
    case 25: /* text */
    case 1043: /* varchar*/
      rv = rb_tainted_str_new(v, length);
      PG_ENCODING_SET_NOCHECK(rv, enc_index);
      break;
    default:
      rv = rb_tainted_str_new(v, length);
      PG_ENCODING_SET_NOCHECK(rv, enc_index);
      if (RTEST(converter)) {
        rv = rb_funcall(converter, spg_id_call, 1, rv);
      }
  }

  return rv;
}

static VALUE spg_array_value(char *c_pg_array_string, int array_string_length, VALUE converter, int enc_index, int oid, VALUE self, VALUE array_type) {
  int index = 1;
  VALUE args[2];
  args[1] = array_type;

  if(RTEST(args[0] = check_pg_array(&index, c_pg_array_string, array_string_length))) {
    return rb_class_new_instance(2, args, spg_PGArray);
  }

  args[0] = read_array(&index, c_pg_array_string, array_string_length, rb_str_buf_new(array_string_length), converter, enc_index, oid, self);
  return rb_class_new_instance(2, args, spg_PGArray);
}

static int spg_time_info_bitmask(void) {
  int info = 0;
  VALUE now = rb_funcall(spg_SQLTime, spg_id_date, 0);

  info = NUM2INT(rb_funcall(now, spg_id_year, 0)) << SPG_YEAR_SHIFT;
  info += NUM2INT(rb_funcall(now, spg_id_month, 0)) << SPG_MONTH_SHIFT;
  info += NUM2INT(rb_funcall(now, spg_id_day, 0));

  if (rb_funcall(spg_Sequel, spg_id_application_timezone, 0) == spg_sym_utc) {
    info += SPG_TIME_UTC;
  }

  return info;
}

static int spg_timestamp_info_bitmask(VALUE self) {
  VALUE db, rtz;
  int tz = SPG_NO_TZ;

  db = rb_funcall(self, spg_id_db, 0);
  rtz = rb_funcall(db, spg_id_timezone, 0);
  if (rtz != Qnil) {
    if (rtz == spg_sym_local) {
      tz += SPG_DB_LOCAL;
    } else if (rtz == spg_sym_utc) {
      tz += SPG_DB_UTC;
    } else {
      tz += SPG_DB_CUSTOM;
    }
  }

  rtz = rb_funcall(spg_Sequel, spg_id_application_timezone, 0);
  if (rtz != Qnil) {
    if (rtz == spg_sym_local) {
      tz += SPG_APP_LOCAL;
    } else if (rtz == spg_sym_utc) {
      tz += SPG_APP_UTC;
    } else {
      tz += SPG_APP_CUSTOM;
    }
  }

  if (rb_cTime == rb_funcall(spg_Sequel, spg_id_datetime_class, 0)) {
    tz += SPG_USE_TIME;
  }

  tz += SPG_TZ_INITIALIZED;
  return tz;
}

static VALUE spg__col_value(VALUE self, PGresult *res, long i, long j, VALUE* colconvert, int enc_index) {
  char *v;
  VALUE rv;
  int ftype = PQftype(res, j);
  VALUE array_type;
  VALUE scalar_oid;
  struct spg_blob_initialization bi;

  if(PQgetisnull(res, i, j)) {
    rv = Qnil;
  } else {
    v = PQgetvalue(res, i, j);

    switch(ftype) {
      case 16: /* boolean */
        rv = *v == 't' ? Qtrue : Qfalse;
        break;
      case 17: /* bytea */
        bi.blob_string = (char *)PQunescapeBytea((unsigned char*)v, &bi.length);
        rv = rb_ensure(spg_create_Blob, (VALUE)&bi, (VALUE(*)())PQfreemem, (VALUE)bi.blob_string);
        break;
      case 20: /* integer */
      case 21:
      case 23:
      case 26:
        rv = pg_text_dec_integer(v, PQgetlength(res, i, j));
        break;
      case 700: /* float */
      case 701:
        if (strcmp("NaN", v) == 0) {
          rv = spg_nan;
        } else if (strcmp("Infinity", v) == 0) {
          rv = spg_pos_inf;
        } else if (strcmp("-Infinity", v) == 0) {
          rv = spg_neg_inf;
        } else {
          rv = rb_float_new(rb_cstr_to_dbl(v, Qfalse));
        }
        break;
      case 1700: /* numeric */
        rv = rb_funcall(spg_Kernel, spg_id_BigDecimal, 1, rb_str_new(v, PQgetlength(res, i, j)));
        break;
      case 1082: /* date */
        rv = spg_date(v, self, PQgetlength(res, i, j));
        break;
      case 1083: /* time */
      case 1266:
        rv = spg_time(v, PQgetlength(res, i, j), (int)colconvert[j]);
        break;
      case 1114: /* timestamp */
      case 1184:
        rv = spg_timestamp(v, self, PQgetlength(res, i, j), (int)colconvert[j]);
        break;
      case 18: /* char */
      case 25: /* text */
      case 1043: /* varchar*/
        rv = rb_tainted_str_new(v, PQgetlength(res, i, j));
        PG_ENCODING_SET_NOCHECK(rv, enc_index);
        break;
      /* array types */
      case 1009:
      case 1014:
      case 1015:
      case 1007:
      case 1115:
      case 1185:
      case 1183:
      case 1270:
      case 1016:
      case 1231:
      case 1022:
      case 1000:
      case 1001:
      case 1182:
      case 1005:
      case 1028:
      case 1021:
      case 143:
      case 791:
      case 1561:
      case 1563:
      case 2951:
      case 1011:
      case 1012:
      case 1003:
      case 1010:
      case 1006:
        switch(ftype) {
          case 1009: 
          case 1014:
            array_type = spg_sym_text;
            scalar_oid = 25;
            break;
          case 1015:
            array_type = spg_sym_character_varying;
            scalar_oid = 25;
            break;
          case 1007:
            array_type = spg_sym_integer;
            scalar_oid = 23;
            break;
          case 1115:
            array_type = spg_sym_timestamp;
            scalar_oid = 1114;
            break;
          case 1185:
            array_type = spg_sym_timestamptz;
            scalar_oid = 1184;
            break;
          case 1183:
            array_type = spg_sym_time;
            scalar_oid = 1083;
            break;
          case 1270:
            array_type = spg_sym_timetz;
            scalar_oid = 1266;
            break;
          case 1016:
            array_type = spg_sym_bigint;
            scalar_oid = 20;
            break;
          case 1231:
            array_type = spg_sym_numeric;
            scalar_oid = 1700;
            break;
          case 1022:
            array_type = spg_sym_double_precision;
            scalar_oid = 701;
            break;
          case 1000:
            array_type = spg_sym_boolean;
            scalar_oid = 16;
            break;
          case 1001:
            array_type = spg_sym_bytea;
            scalar_oid = 17;
            break;
          case 1182:
            array_type = spg_sym_date;
            scalar_oid = 1082;
            break;
          case 1005:
            array_type = spg_sym_smallint;
            scalar_oid = 21;
            break;
          case 1028:
            array_type = spg_sym_oid;
            scalar_oid = 26;
            break;
          case 1021:
            array_type = spg_sym_real;
            scalar_oid = 700;
            break;
          case 143:
            array_type = spg_sym_xml;
            scalar_oid = 142;
            break;
          case 791:
            array_type = spg_sym_money;
            scalar_oid = 790;
            break;
          case 1561:
            array_type = spg_sym_bit;
            scalar_oid = 1560;
            break;
          case 1563:
            array_type = spg_sym_bit_varying;
            scalar_oid = 1562;
            break;
          case 2951:
            array_type = spg_sym_uuid;
            scalar_oid = 2950;
            break;
          case 1011:
            array_type = spg_sym_xid;
            scalar_oid = 28;
            break;
          case 1012:
            array_type = spg_sym_cid;
            scalar_oid = 29;
            break;
          case 1003:
            array_type = spg_sym_name;
            scalar_oid = 19;
            break;
          case 1010:
            array_type = spg_sym_tid;
            scalar_oid = 27;
            break;
          case 1006:
            array_type = spg_sym_int2vector;
            scalar_oid = 22;
            break;
        }
        rv = spg_array_value(v, PQgetlength(res, i, j), colconvert[j], enc_index, scalar_oid, self, array_type);
        break;
      default:
        rv = rb_tainted_str_new(v, PQgetlength(res, i, j));
        PG_ENCODING_SET_NOCHECK(rv, enc_index);
        if (colconvert[j] != Qnil) {
          rv = rb_funcall(colconvert[j], spg_id_call, 1, rv); 
        }
    }
  }
  return rv;
}

static VALUE spg__col_values(VALUE self, VALUE v, VALUE *colsyms, long nfields, PGresult *res, long i, VALUE *colconvert, int enc_index) {
  long j;
  VALUE cur;
  long len = RARRAY_LEN(v);
  VALUE a = rb_ary_new2(len);
  for (j=0; j<len; j++) {
    cur = rb_ary_entry(v, j);
    rb_ary_store(a, j, cur == Qnil ? Qnil : spg__col_value(self, res, i, NUM2LONG(cur), colconvert, enc_index));
  }
  return a;
}

static long spg__field_id(VALUE v, VALUE *colsyms, long nfields) {
  long j;
  for (j=0; j<nfields; j++) {
    if (colsyms[j] == v) {
      return j;
    }
  }
  return -1;
}

static VALUE spg__field_ids(VALUE v, VALUE *colsyms, long nfields) {
  long i;
  long j;
  VALUE cur;
  long len = RARRAY_LEN(v);
  VALUE pg_columns = rb_ary_new2(len);
  for (i=0; i<len; i++) {
    cur = rb_ary_entry(v, i);
    j = spg__field_id(cur, colsyms, nfields);
    rb_ary_store(pg_columns, i, j == -1 ? Qnil : LONG2NUM(j));
  }
  return pg_columns;
}

static void spg_set_column_info(VALUE self, PGresult *res, VALUE *colsyms, VALUE *colconvert) {
  long i;
  long j;
  long nfields;
  int timestamp_info = 0;
  int time_info = 0;
  VALUE conv_procs = 0;

  nfields = PQnfields(res);

  for(j=0; j<nfields; j++) {
    colsyms[j] = rb_funcall(self, spg_id_output_identifier, 1, rb_str_new2(PQfname(res, j)));
    i = PQftype(res, j);
    switch (i) {
      /* scalar types */
      case 16:
      case 17:
      case 20:
      case 21:
      case 23:
      case 26:
      case 700:
      case 701:
      case 790:
      case 1700:
      case 1082:
      case 18:
      case 25:
      case 1043:
      /* array types */
      case 1009:
      case 1014:
      case 1015:
      case 1007:
      case 1016:
      case 1231:
      case 1022:
      case 1000:
      case 1001:
      case 1182:
      case 1005:
      case 1028:
      case 1021:
      case 143:
      case 791:
      case 1561:
      case 1563:
      case 2951:
      case 1011:
      case 1012:
      case 1003:
      case 1010:
        colconvert[j] = Qnil;
        break;

      /* time types */
      case 1183:
      case 1083:
      case 1270:
      case 1266:
        if (time_info == 0) {
          time_info = spg_time_info_bitmask();
        }
        colconvert[j] = time_info;
        break;

      /* timestamp types */
      case 1115:
      case 1185:
      case 1114:
      case 1184:
        if (timestamp_info == 0) {
          timestamp_info = spg_timestamp_info_bitmask(self);
        }
        colconvert[j] = (VALUE)((i == 1184 || i == 1185) ? (timestamp_info + SPG_HAS_TIMEZONE) : timestamp_info);
        break;

      default:
        if (conv_procs == 0) {
          conv_procs = rb_funcall(rb_funcall(self, spg_id_db, 0), spg_id_conversion_procs, 0);
        }
        colconvert[j] = rb_funcall(conv_procs, spg_id_get, 1, INT2NUM(i));
        break;
    }
  }

  rb_funcall(self, spg_id_columns_equal, 1, rb_ary_new4(nfields, colsyms));
}

static VALUE spg_yield_hash_rows(VALUE self, VALUE rres, VALUE ignore) {
  PGresult *res;
  VALUE colsyms[SPG_MAX_FIELDS];
  VALUE colconvert[SPG_MAX_FIELDS];
  long ntuples;
  long nfields;
  long i;
  long j;
  VALUE h;
  VALUE opts;
  VALUE pg_type;
  VALUE pg_value;
  char type = SPG_YIELD_NORMAL;

  if (!RTEST(rres)) {
    return self;
  }
  res = pgresult_get(rres);

  int enc_index;
  enc_index = enc_get_index(rres);

  ntuples = PQntuples(res);
  nfields = PQnfields(res);
  if (nfields > SPG_MAX_FIELDS) {
    rb_raise(rb_eRangeError, "more than %d columns in query (%ld columns detected)", SPG_MAX_FIELDS, nfields);
  }

  spg_set_column_info(self, res, colsyms, colconvert);

  opts = rb_funcall(self, spg_id_opts, 0);
  if (rb_type(opts) == T_HASH) {
    pg_type = rb_hash_aref(opts, spg_sym__sequel_pg_type);
    pg_value = rb_hash_aref(opts, spg_sym__sequel_pg_value);
    if (SYMBOL_P(pg_type)) {
      if (pg_type == spg_sym_map) {
        if (SYMBOL_P(pg_value)) {
          type = SPG_YIELD_COLUMN;
        } else if (rb_type(pg_value) == T_ARRAY) {
          type = SPG_YIELD_COLUMNS;
        }
      } else if (pg_type == spg_sym_first) {
        type = SPG_YIELD_FIRST;
      } else if (pg_type == spg_sym_array) {
        type = SPG_YIELD_ARRAY;
      } else if ((pg_type == spg_sym_hash || pg_type == spg_sym_hash_groups) && rb_type(pg_value) == T_ARRAY) {
        VALUE pg_value_key, pg_value_value;
        pg_value_key = rb_ary_entry(pg_value, 0);
        pg_value_value = rb_ary_entry(pg_value, 1);
        if (SYMBOL_P(pg_value_key)) {
          if (SYMBOL_P(pg_value_value)) {
            type = pg_type == spg_sym_hash_groups ? SPG_YIELD_KV_HASH_GROUPS : SPG_YIELD_KV_HASH;
          } else if (rb_type(pg_value_value) == T_ARRAY) {
            type = pg_type == spg_sym_hash_groups ? SPG_YIELD_KMV_HASH_GROUPS : SPG_YIELD_KMV_HASH;
          }
        } else if (rb_type(pg_value_key) == T_ARRAY) {
          if (SYMBOL_P(pg_value_value)) {
            type = pg_type == spg_sym_hash_groups ? SPG_YIELD_MKV_HASH_GROUPS : SPG_YIELD_MKV_HASH;
          } else if (rb_type(pg_value_value) == T_ARRAY) {
            type = pg_type == spg_sym_hash_groups ? SPG_YIELD_MKMV_HASH_GROUPS : SPG_YIELD_MKMV_HASH;
          }
        }
      } else if (pg_type == spg_sym_model && rb_type(pg_value) == T_CLASS) {
        type = SPG_YIELD_MODEL;
      }
    }
  }

  switch(type) {
    case SPG_YIELD_NORMAL:
      /* Normal, hash for entire row */
      for(i=0; i<ntuples; i++) {
        h = rb_hash_new();
        for(j=0; j<nfields; j++) {
          rb_hash_aset(h, colsyms[j], spg__col_value(self, res, i, j, colconvert, enc_index));
        }
        rb_yield(h);
      }
      break;
    case SPG_YIELD_COLUMN:
      /* Single column */
      j = spg__field_id(pg_value, colsyms, nfields);
      if (j == -1) {
        for(i=0; i<ntuples; i++) {
          rb_yield(Qnil);
        } 
      } else {
        for(i=0; i<ntuples; i++) {
          rb_yield(spg__col_value(self, res, i, j, colconvert, enc_index));
        } 
      }
      break;
    case SPG_YIELD_COLUMNS:
      /* Multiple columns as an array */
      h = spg__field_ids(pg_value, colsyms, nfields);
      for(i=0; i<ntuples; i++) {
        rb_yield(spg__col_values(self, h, colsyms, nfields, res, i, colconvert, enc_index));
      } 
      break;
    case SPG_YIELD_FIRST:
      /* First column */
      for(i=0; i<ntuples; i++) {
        rb_yield(spg__col_value(self, res, i, 0, colconvert, enc_index));
      } 
      break;
    case SPG_YIELD_ARRAY:
      /* Array of all columns */
      for(i=0; i<ntuples; i++) {
        h = rb_ary_new2(nfields);
        for(j=0; j<nfields; j++) {
          rb_ary_store(h, j, spg__col_value(self, res, i, j, colconvert, enc_index));
        }
        rb_yield(h);
      } 
      break;
    case SPG_YIELD_KV_HASH:
    case SPG_YIELD_KV_HASH_GROUPS:
      /* Hash with single key and single value */
      {
        VALUE k, v;
        h = rb_hash_new();
        k = spg__field_id(rb_ary_entry(pg_value, 0), colsyms, nfields);
        v = spg__field_id(rb_ary_entry(pg_value, 1), colsyms, nfields);
        if(type == SPG_YIELD_KV_HASH) {
          for(i=0; i<ntuples; i++) {
            rb_hash_aset(h, spg__col_value(self, res, i, k, colconvert, enc_index), spg__col_value(self, res, i, v, colconvert, enc_index));
          } 
        } else {
          VALUE kv, vv, a;
          for(i=0; i<ntuples; i++) {
            kv = spg__col_value(self, res, i, k, colconvert, enc_index);
            vv = spg__col_value(self, res, i, v, colconvert, enc_index);
            a = rb_hash_lookup(h, kv);
            if(!RTEST(a)) {
              rb_hash_aset(h, kv, rb_ary_new3(1, vv));
            } else {
              rb_ary_push(a, vv);
            }
          } 
        }
        rb_yield(h);
      }
      break;
    case SPG_YIELD_MKV_HASH:
    case SPG_YIELD_MKV_HASH_GROUPS:
      /* Hash with array of keys and single value */
      {
        VALUE k, v;
        h = rb_hash_new();
        k = spg__field_ids(rb_ary_entry(pg_value, 0), colsyms, nfields);
        v = spg__field_id(rb_ary_entry(pg_value, 1), colsyms, nfields);
        if(type == SPG_YIELD_MKV_HASH) {
          for(i=0; i<ntuples; i++) {
            rb_hash_aset(h, spg__col_values(self, k, colsyms, nfields, res, i, colconvert, enc_index), spg__col_value(self, res, i, v, colconvert, enc_index));
          } 
        } else {
          VALUE kv, vv, a;
          for(i=0; i<ntuples; i++) {
            kv = spg__col_values(self, k, colsyms, nfields, res, i, colconvert, enc_index);
            vv = spg__col_value(self, res, i, v, colconvert, enc_index);
            a = rb_hash_lookup(h, kv);
            if(!RTEST(a)) {
              rb_hash_aset(h, kv, rb_ary_new3(1, vv));
            } else {
              rb_ary_push(a, vv);
            }
          } 
        }
        rb_yield(h);
      }
      break;
    case SPG_YIELD_KMV_HASH:
    case SPG_YIELD_KMV_HASH_GROUPS:
      /* Hash with single keys and array of values */
      {
        VALUE k, v;
        h = rb_hash_new();
        k = spg__field_id(rb_ary_entry(pg_value, 0), colsyms, nfields);
        v = spg__field_ids(rb_ary_entry(pg_value, 1), colsyms, nfields);
        if(type == SPG_YIELD_KMV_HASH) {
          for(i=0; i<ntuples; i++) {
            rb_hash_aset(h, spg__col_value(self, res, i, k, colconvert, enc_index), spg__col_values(self, v, colsyms, nfields, res, i, colconvert, enc_index));
          } 
        } else {
          VALUE kv, vv, a;
          for(i=0; i<ntuples; i++) {
            kv = spg__col_value(self, res, i, k, colconvert, enc_index);
            vv = spg__col_values(self, v, colsyms, nfields, res, i, colconvert, enc_index);
            a = rb_hash_lookup(h, kv);
            if(!RTEST(a)) {
              rb_hash_aset(h, kv, rb_ary_new3(1, vv));
            } else {
              rb_ary_push(a, vv);
            }
          } 
        }
        rb_yield(h);
      }
      break;
    case SPG_YIELD_MKMV_HASH:
    case SPG_YIELD_MKMV_HASH_GROUPS:
      /* Hash with array of keys and array of values */
      {
        VALUE k, v;
        h = rb_hash_new();
        k = spg__field_ids(rb_ary_entry(pg_value, 0), colsyms, nfields);
        v = spg__field_ids(rb_ary_entry(pg_value, 1), colsyms, nfields);
        if(type == SPG_YIELD_MKMV_HASH) {
          for(i=0; i<ntuples; i++) {
            rb_hash_aset(h, spg__col_values(self, k, colsyms, nfields, res, i, colconvert, enc_index), spg__col_values(self, v, colsyms, nfields, res, i, colconvert, enc_index));
          } 
        } else {
          VALUE kv, vv, a;
          for(i=0; i<ntuples; i++) {
            kv = spg__col_values(self, k, colsyms, nfields, res, i, colconvert, enc_index);
            vv = spg__col_values(self, v, colsyms, nfields, res, i, colconvert, enc_index);
            a = rb_hash_lookup(h, kv);
            if(!RTEST(a)) {
              rb_hash_aset(h, kv, rb_ary_new3(1, vv));
            } else {
              rb_ary_push(a, vv);
            }
          } 
        }
        rb_yield(h);
      }
      break;
    case SPG_YIELD_MODEL:
      /* Model object for entire row */
      for(i=0; i<ntuples; i++) {
        h = rb_hash_new();
        for(j=0; j<nfields; j++) {
          rb_hash_aset(h, colsyms[j], spg__col_value(self, res, i, j, colconvert, enc_index));
        }
        /* Abuse local variable */
        pg_type = rb_obj_alloc(pg_value);
        rb_ivar_set(pg_type, spg_id_values, h);
        rb_yield(pg_type);
      }
      break;
  }
  return self;
}

static VALUE spg_supports_streaming_p(VALUE self) {
  return
#if HAVE_PQSETSINGLEROWMODE
  Qtrue;
#else
  Qfalse;
#endif
}

#if HAVE_PQSETSINGLEROWMODE
static VALUE spg_set_single_row_mode(VALUE self) {
  PGconn *conn;
  conn = pg_get_pgconn(self);
  if (PQsetSingleRowMode(conn) != 1) {
      rb_raise(spg_PGError, "cannot set single row mode");
  }
  return Qnil;
}

static VALUE spg__yield_each_row(VALUE self) {
  PGconn *conn;
  PGresult *res;
  VALUE rres;
  VALUE rconn;
  VALUE colsyms[SPG_MAX_FIELDS];
  VALUE colconvert[SPG_MAX_FIELDS];
  long nfields;
  long j;
  VALUE h;
  VALUE opts;
  VALUE pg_type;
  VALUE pg_value = Qnil;
  char type = SPG_YIELD_NORMAL;

  rconn = rb_ary_entry(self, 1);
  self = rb_ary_entry(self, 0);
  conn = pg_get_pgconn(rconn);

  rres = rb_funcall(rconn, spg_id_get_result, 0);
  if (rres == Qnil) {
    goto end_yield_each_row;
  }
  rb_funcall(rres, spg_id_check, 0);
  res = pgresult_get(rres);

  int enc_index;
  enc_index = enc_get_index(rres);

  /* Only handle regular and model types.  All other types require compiling all
   * of the results at once, which is not a use case for streaming.  The streaming
   * code does not call this function for the other types. */
  opts = rb_funcall(self, spg_id_opts, 0);
  if (rb_type(opts) == T_HASH) {
    pg_type = rb_hash_aref(opts, spg_sym__sequel_pg_type);
    pg_value = rb_hash_aref(opts, spg_sym__sequel_pg_value);
    if (SYMBOL_P(pg_type) && pg_type == spg_sym_model && rb_type(pg_value) == T_CLASS) {
      type = SPG_YIELD_MODEL;
    }
  }

  nfields = PQnfields(res);
  if (nfields > SPG_MAX_FIELDS) {
    rb_funcall(rres, spg_id_clear, 0);
    rb_raise(rb_eRangeError, "more than %d columns in query", SPG_MAX_FIELDS);
  }

  spg_set_column_info(self, res, colsyms, colconvert);

  while (PQntuples(res) != 0) {
    h = rb_hash_new();
    for(j=0; j<nfields; j++) {
      rb_hash_aset(h, colsyms[j], spg__col_value(self, res, 0, j, colconvert , enc_index));
    }

    rb_funcall(rres, spg_id_clear, 0);

    if(type == SPG_YIELD_MODEL) {
      /* Abuse local variable */
      pg_type = rb_obj_alloc(pg_value);
      rb_ivar_set(pg_type, spg_id_values, h);
      rb_yield(pg_type);
    } else {
      rb_yield(h);
    }

    rres = rb_funcall(rconn, spg_id_get_result, 0);
    if (rres == Qnil) {
      goto end_yield_each_row;
    }
    rb_funcall(rres, spg_id_check, 0);
    res = pgresult_get(rres);
  }
  rb_funcall(rres, spg_id_clear, 0);

end_yield_each_row:
  return self;
}

static VALUE spg__flush_results(VALUE rconn) {
  PGconn *conn;
  PGresult *res;
  VALUE error = 0;
  conn = pg_get_pgconn(rconn);

  while ((res = PQgetResult(conn)) != NULL) {
    if (!error) {
      switch (PQresultStatus(res))
      {
        case PGRES_BAD_RESPONSE:
        case PGRES_FATAL_ERROR:
        case PGRES_NONFATAL_ERROR:
          error = rb_str_new2(PQresultErrorMessage(res));
          break;
        default:
          break;
      }
    }
    PQclear(res);
  }
  
  if (error) {
    VALUE exception = rb_exc_new3(spg_PGError, error);
    rb_iv_set(exception, "@connection", rconn);
    rb_exc_raise(exception);
  }

  return rconn;
}

static VALUE spg_yield_each_row(VALUE self, VALUE rconn) {
  VALUE v;
  v = rb_ary_new3(2, self, rconn);
  return rb_ensure(spg__yield_each_row, v, spg__flush_results, rconn);
}
#endif

void Init_sequel_pg(void) {
  VALUE c, spg_Postgres;
  ID cg;
  cg = rb_intern("const_get");

  spg_Sequel = rb_funcall(rb_cObject, cg, 1, rb_str_new2("Sequel"));
  spg_Postgres = rb_funcall(spg_Sequel, cg, 1, rb_str_new2("Postgres"));

  if(rb_obj_respond_to(spg_Postgres, rb_intern("sequel_pg_version_supported?"), 0)) {
    if(!RTEST(rb_funcall(spg_Postgres, rb_intern("sequel_pg_version_supported?"), 1, INT2FIX(SEQUEL_PG_VERSION_INTEGER)))) {
      rb_warn("sequel_pg not loaded as it is not compatible with the Sequel version in use; install the latest version of sequel_pg or uninstall sequel_pg");
      return;
    }
  }
  rb_funcall(spg_Postgres, rb_intern("const_set"), 2, ID2SYM(rb_intern("SEQUEL_PG_VERSION_INTEGER")), INT2FIX(SEQUEL_PG_VERSION_INTEGER));

  spg_id_BigDecimal = rb_intern("BigDecimal");
  spg_id_new = rb_intern("new");
  spg_id_date = rb_intern("date");
  spg_id_local = rb_intern("local");
  spg_id_year = rb_intern("year");
  spg_id_month = rb_intern("month");
  spg_id_day = rb_intern("day");
  spg_id_output_identifier = rb_intern("output_identifier");
  spg_id_datetime_class = rb_intern("datetime_class");
  spg_id_application_timezone = rb_intern("application_timezone");
  spg_id_to_application_timestamp = rb_intern("to_application_timestamp");
  spg_id_timezone = rb_intern("timezone");
  spg_id_op_plus = rb_intern("+");
  spg_id_utc = rb_intern("utc");
  spg_id_utc_offset = rb_intern("utc_offset");
  spg_id_localtime = rb_intern("localtime");
  spg_id_new_offset = rb_intern("new_offset");
  spg_id_convert_infinite_timestamps = rb_intern("convert_infinite_timestamps");
  spg_id_infinite_timestamp_value = rb_intern("infinite_timestamp_value");

  spg_id_call = rb_intern("call");
  spg_id_get = rb_intern("[]");

  spg_id_opts = rb_intern("opts");

  spg_id_db = rb_intern("db");
  spg_id_conversion_procs = rb_intern("conversion_procs");
  spg_id_columns_equal = rb_intern("columns=");

  spg_id_columns = rb_intern("@columns");
  spg_id_encoding = rb_intern("@encoding");
  spg_id_values = rb_intern("@values");

  spg_sym_utc = ID2SYM(rb_intern("utc"));
  spg_sym_local = ID2SYM(rb_intern("local"));
  spg_sym_map = ID2SYM(rb_intern("map"));
  spg_sym_first = ID2SYM(rb_intern("first"));
  spg_sym_array = ID2SYM(rb_intern("array"));
  spg_sym_hash = ID2SYM(rb_intern("hash"));
  spg_sym_hash_groups = ID2SYM(rb_intern("hash_groups"));
  spg_sym_model = ID2SYM(rb_intern("model"));
  spg_sym__sequel_pg_type = ID2SYM(rb_intern("_sequel_pg_type"));
  spg_sym__sequel_pg_value = ID2SYM(rb_intern("_sequel_pg_value"));

  spg_sym_text = ID2SYM(rb_intern("text"));
  spg_sym_character_varying = ID2SYM(rb_intern("character varying"));
  spg_sym_integer = ID2SYM(rb_intern("integer"));
  spg_sym_timestamp = ID2SYM(rb_intern("timestamp"));
  spg_sym_timestamptz = ID2SYM(rb_intern("timestamptz"));
  spg_sym_time = ID2SYM(rb_intern("time"));
  spg_sym_timetz = ID2SYM(rb_intern("timetz"));
  spg_sym_bigint = ID2SYM(rb_intern("bigint"));
  spg_sym_numeric = ID2SYM(rb_intern("numeric"));
  spg_sym_double_precision = ID2SYM(rb_intern("double precision"));
  spg_sym_boolean = ID2SYM(rb_intern("boolean"));
  spg_sym_bytea = ID2SYM(rb_intern("bytea"));
  spg_sym_date = ID2SYM(rb_intern("date"));
  spg_sym_smallint = ID2SYM(rb_intern("smallint"));
  spg_sym_oid = ID2SYM(rb_intern("oid"));
  spg_sym_real = ID2SYM(rb_intern("real"));
  spg_sym_xml = ID2SYM(rb_intern("xml"));
  spg_sym_money = ID2SYM(rb_intern("money"));
  spg_sym_bit = ID2SYM(rb_intern("bit"));
  spg_sym_bit_varying = ID2SYM(rb_intern("bit varying"));
  spg_sym_uuid = ID2SYM(rb_intern("uuid"));
  spg_sym_xid = ID2SYM(rb_intern("xid"));
  spg_sym_cid = ID2SYM(rb_intern("cid"));
  spg_sym_name = ID2SYM(rb_intern("name"));
  spg_sym_tid = ID2SYM(rb_intern("tid"));
  spg_sym_int2vector = ID2SYM(rb_intern("int2vector"));

  spg_Blob = rb_funcall(rb_funcall(spg_Sequel, cg, 1, rb_str_new2("SQL")), cg, 1, rb_str_new2("Blob")); 
  spg_Blob_instance = rb_obj_freeze(rb_funcall(spg_Blob, spg_id_new, 0));
  spg_SQLTime= rb_funcall(spg_Sequel, cg, 1, rb_str_new2("SQLTime")); 
  spg_Kernel = rb_funcall(rb_cObject, cg, 1, rb_str_new2("Kernel")); 
  spg_Date = rb_funcall(rb_cObject, cg, 1, rb_str_new2("Date")); 
  spg_DateTime = rb_funcall(rb_cObject, cg, 1, rb_str_new2("DateTime")); 
  spg_PGError = rb_funcall(rb_funcall(rb_cObject, cg, 1, rb_str_new2("PG")), cg, 1, rb_str_new2("Error"));

  spg_nan = rb_eval_string("0.0/0.0");
  spg_pos_inf = rb_eval_string("1.0/0.0");
  spg_neg_inf = rb_eval_string("-1.0/0.0");
  spg_usec_per_day = ULL2NUM(86400000000ULL);

  rb_global_variable(&spg_Sequel);
  rb_global_variable(&spg_Blob);
  rb_global_variable(&spg_Blob_instance);
  rb_global_variable(&spg_Kernel);
  rb_global_variable(&spg_Date);
  rb_global_variable(&spg_DateTime);
  rb_global_variable(&spg_SQLTime);
  rb_global_variable(&spg_PGError);
  rb_global_variable(&spg_nan);
  rb_global_variable(&spg_pos_inf);
  rb_global_variable(&spg_neg_inf);
  rb_global_variable(&spg_usec_per_day);

  c = rb_funcall(spg_Postgres, cg, 1, rb_str_new2("Dataset"));
  rb_undef_method(c, "yield_hash_rows");
  rb_define_private_method(c, "yield_hash_rows", spg_yield_hash_rows, 2);
  rb_undef_method(c, "fetch_rows_set_cols");
  rb_define_private_method(c, "fetch_rows_set_cols", spg_fetch_rows_set_cols, 1);

  rb_define_singleton_method(spg_Postgres, "supports_streaming?", spg_supports_streaming_p, 0);

#if HAVE_PQSETSINGLEROWMODE
  spg_id_get_result = rb_intern("get_result");
  spg_id_clear = rb_intern("clear");
  spg_id_check = rb_intern("check");

  rb_define_private_method(c, "yield_each_row", spg_yield_each_row, 1);
  c = rb_funcall(spg_Postgres, cg, 1, rb_str_new2("Adapter"));
  rb_define_private_method(c, "set_single_row_mode", spg_set_single_row_mode, 0);
#endif

  rb_define_singleton_method(spg_Postgres, "parse_pg_array", parse_pg_array, 2);

  rb_require("sequel_pg/sequel_pg");

  rb_require("sequel/extensions/pg_array");
  spg_PGArray = rb_funcall(spg_Postgres, cg, 1, rb_str_new2("PGArray"));
  rb_global_variable(&spg_PGArray);
}
