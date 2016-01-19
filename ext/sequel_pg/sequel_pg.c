#include <string.h>
#include <stdio.h>
#include <math.h>
#include <libpq-fe.h>
#include <ruby.h>

#if defined(HAVE_RUBY_ENCODING_H) && HAVE_RUBY_ENCODING_H
#define SPG_ENCODING 1
#include <ruby/encoding.h>
#define ENC_INDEX ,enc_index
#else
#define ENC_INDEX
#endif

#ifndef SPG_MAX_FIELDS
#define SPG_MAX_FIELDS 256
#endif
#define SPG_MICROSECONDS_PER_DAY_LL 86400000000ULL
#define SPG_MICROSECONDS_PER_DAY 86400000000.0
#define SPG_MINUTES_PER_DAY 1440.0
#define SPG_SECONDS_PER_DAY 86400.0

#define SPG_DT_ADD_USEC if (usec != 0) { dt = rb_funcall(dt, spg_id_op_plus, 1, spg_id_Rational ? rb_funcall(rb_cObject, spg_id_Rational, 2, INT2NUM(usec), ULL2NUM(SPG_MICROSECONDS_PER_DAY_LL)) : rb_float_new(usec/SPG_MICROSECONDS_PER_DAY)); }

#define SPG_NO_TZ 0
#define SPG_DB_LOCAL 1
#define SPG_DB_UTC 2
#define SPG_APP_LOCAL 4
#define SPG_APP_UTC 8

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

/* Whether the data objects are structs instead of just pointers */
static int unwrap_structs;

/* External functions defined by ruby-pg when data objects are structs */
PGconn* pg_get_pgconn(VALUE);
PGresult* pgresult_get(VALUE);

/* Normalize access to data objects for both old and new versions of pg gem */
#define GetPGconn(_val, _var) if (unwrap_structs) {Check_Type(_val, T_DATA); _var = pg_get_pgconn(_val);} else {Data_Get_Struct(_val, PGconn, _var);}
#define GetPGresult(_val, _var) if (unwrap_structs) {Check_Type(_val, T_DATA); _var = pgresult_get(_val);} else {Data_Get_Struct(_val, PGresult, _var);}

static VALUE spg_Sequel;
static VALUE spg_Blob;
static VALUE spg_BigDecimal;
static VALUE spg_Date;
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

static VALUE spg_nan;
static VALUE spg_pos_inf;
static VALUE spg_neg_inf;

static ID spg_id_Rational;
static ID spg_id_new;
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

static ID spg_id_columns;
static ID spg_id_encoding;
static ID spg_id_values;

#if HAVE_PQSETSINGLEROWMODE
static ID spg_id_get_result;
static ID spg_id_clear;
static ID spg_id_check;
#endif

#if SPG_ENCODING
static int enc_get_index(VALUE val)
{
  int i = ENCODING_GET_INLINED(val);
  if (i == ENCODING_INLINE_MAX) {
    i = NUM2INT(rb_ivar_get(val, spg_id_encoding));
  }
  return i;
}
#endif

static VALUE read_array(int *index, char *c_pg_array_string, int array_string_length, char *word, VALUE converter
#ifdef SPG_ENCODING
, int enc_index
#endif
)
{
  int word_index = 0;

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
            VALUE rword = rb_tainted_str_new(word, word_index);
            RB_GC_GUARD(rword);

#ifdef SPG_ENCODING
            rb_enc_associate_index(rword, enc_index);
#endif

            if (RTEST(converter)) {
              rword = rb_funcall(converter, spg_id_call, 1, rword);
            }

            rb_ary_push(array, rword);
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
        rb_ary_push(array, read_array(index, c_pg_array_string, array_string_length, word, converter
#ifdef SPG_ENCODING
, enc_index
#endif
        ));
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

  return array;
}

static VALUE parse_pg_array(VALUE self, VALUE pg_array_string, VALUE converter) {

  /* convert to c-string, create additional ruby string buffer of
   * the same length, as that will be the worst case. */
  char *c_pg_array_string = StringValueCStr(pg_array_string);
  int array_string_length = RSTRING_LEN(pg_array_string);
  VALUE buf = rb_str_buf_new(array_string_length);
  RB_GC_GUARD(buf);
  char *word = RSTRING_PTR(buf);
  int index = 1;

  if (array_string_length == 0) {
    rb_raise(rb_eArgError, "unexpected PostgreSQL array format, empty");
  }

  switch (c_pg_array_string[0]) {
    case '[':
      /* Skip explicit subscripts, scanning until opening array */
      for(;index < array_string_length && c_pg_array_string[index] != '{'; ++index)
        /* nothing */;

      if (index >= array_string_length) {
        rb_raise(rb_eArgError, "unexpected PostgreSQL array format, no {");
      } else {
        ++index;
      }
    case '{':
      break;
    default:
      rb_raise(rb_eArgError, "unexpected PostgreSQL array format, doesn't start with { or [");
  }

  return read_array(&index, c_pg_array_string, array_string_length, word, converter
#ifdef SPG_ENCODING
, enc_get_index(pg_array_string)
#endif
  );
}

static VALUE spg_time(const char *s) {
  VALUE now;
  int hour, minute, second, tokens, i;
  char subsec[7];
  int usec = 0;

  tokens = sscanf(s, "%2d:%2d:%2d.%6s", &hour, &minute, &second, subsec);
  if(tokens == 4) {
    for(i=0; i<6; i++) {
      if(subsec[i] == '-') {
        subsec[i] = '\0';
      }
    }
    usec = atoi(subsec);
    usec *= (int) pow(10, (6 - strlen(subsec)));
  } else if(tokens < 3) {
    rb_raise(rb_eArgError, "unexpected time format");
  }

  now = rb_funcall(spg_SQLTime, spg_id_new, 0);
  return rb_funcall(spg_SQLTime, spg_id_local, 7, rb_funcall(now, spg_id_year, 0), rb_funcall(now, spg_id_month, 0), rb_funcall(now, spg_id_day, 0), INT2NUM(hour), INT2NUM(minute), INT2NUM(second), INT2NUM(usec));
}

static VALUE spg_timestamp_error(const char *s, VALUE self, const char *error_msg) {
  VALUE db;
  db = rb_funcall(self, spg_id_db, 0);
  if(RTEST(rb_funcall(db, spg_id_convert_infinite_timestamps, 0))) {
    if((strcmp(s, "infinity") == 0) || (strcmp(s, "-infinity") == 0)) {
      return rb_funcall(db, spg_id_infinite_timestamp_value, 1, rb_tainted_str_new2(s));
    }
  }
  rb_raise(rb_eArgError, "%s", error_msg);
}

static VALUE spg_date(const char *s, VALUE self) {
  int year, month, day;

  if(3 != sscanf(s, "%d-%2d-%2d", &year, &month, &day)) {
    return spg_timestamp_error(s, self, "unexpected date format");
  }

  return rb_funcall(spg_Date, spg_id_new, 3, INT2NUM(year), INT2NUM(month), INT2NUM(day));
}

static VALUE spg_timestamp(const char *s, VALUE self) {
  VALUE dtc, dt, rtz, db;
  int tz = SPG_NO_TZ;
  int year, month, day, hour, min, sec, usec, tokens, utc_offset;
  int usec_start, usec_stop;
  char offset_sign = 0;
  int offset_hour = 0;
  int offset_minute = 0;
  int offset_seconds = 0;
  double offset_fraction = 0.0;

  db = rb_funcall(self, spg_id_db, 0);
  rtz = rb_funcall(db, spg_id_timezone, 0);
  if (rtz != Qnil) {
    if (rtz == spg_sym_local) {
      tz += SPG_DB_LOCAL;
    } else if (rtz == spg_sym_utc) {
      tz += SPG_DB_UTC;
    } else {
      return rb_funcall(db, spg_id_to_application_timestamp, 1, rb_str_new2(s)); 
    }
  }

  rtz = rb_funcall(spg_Sequel, spg_id_application_timezone, 0);
  if (rtz != Qnil) {
    if (rtz == spg_sym_local) {
      tz += SPG_APP_LOCAL;
    } else if (rtz == spg_sym_utc) {
      tz += SPG_APP_UTC;
    } else {
      return rb_funcall(db, spg_id_to_application_timestamp, 1, rb_str_new2(s)); 
    }
  }

  if (0 != strchr(s, '.')) {
    tokens = sscanf(s, "%d-%2d-%2d %2d:%2d:%2d.%n%d%n%c%02d:%02d", 
	&year, &month, &day, &hour, &min, &sec,
       	&usec_start, &usec, &usec_stop, 
	&offset_sign, &offset_hour, &offset_minute);
    if(tokens < 7) {
      return spg_timestamp_error(s, self, "unexpected datetime format");
    }
    usec *= (int) pow(10, (6 - (usec_stop - usec_start)));
  } else {
    tokens = sscanf(s, "%d-%2d-%2d %2d:%2d:%2d%c%02d:%02d", 
	&year, &month, &day, &hour, &min, &sec,
	&offset_sign, &offset_hour, &offset_minute);
    if (tokens == 3) {
      hour = 0;
      min = 0;
      sec = 0;
    } else if (tokens < 6) {
      return spg_timestamp_error(s, self, "unexpected datetime format");
    }
    usec = 0;
  }

  if (offset_sign == '-') {
    offset_hour *= -1;
    offset_minute *= -1;
  }

  dtc = rb_funcall(spg_Sequel, spg_id_datetime_class, 0);

  if (dtc == rb_cTime) {
    if (offset_sign) {
      /* Offset given, convert to local time if not already in local time.
       * While PostgreSQL generally returns timestamps in local time, it's unwise to rely on this.
       */
      dt = rb_funcall(rb_cTime, spg_id_local, 7, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec), INT2NUM(usec));
      utc_offset = NUM2INT(rb_funcall(dt, spg_id_utc_offset, 0));
      offset_seconds = offset_hour * 3600 + offset_minute * 60;
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
    if (offset_sign) {
      /* Offset given, handle correct local time.
       * While PostgreSQL generally returns timestamps in local time, it's unwise to rely on this.
       */
      offset_fraction = offset_hour/24.0 + offset_minute/SPG_MINUTES_PER_DAY;
      dt = rb_funcall(dtc, spg_id_new, 7, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec), rb_float_new(offset_fraction));
      SPG_DT_ADD_USEC

      if (tz & SPG_APP_LOCAL) {
        utc_offset = NUM2INT(rb_funcall(rb_funcall(rb_cTime, spg_id_new, 0), spg_id_utc_offset, 0))/SPG_SECONDS_PER_DAY;
        dt = rb_funcall(dt, spg_id_new_offset, 1, rb_float_new(utc_offset));
      } else if (tz & SPG_APP_UTC) {
        dt = rb_funcall(dt, spg_id_new_offset, 1, INT2NUM(0));
      } 
      return dt;
    } else if (tz == SPG_NO_TZ) {
      dt = rb_funcall(dtc, spg_id_new, 6, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec));
      SPG_DT_ADD_USEC
      return dt;
    }

    /* No offset given, and some timezone combination given */
    if (tz & SPG_DB_LOCAL) {
      offset_fraction = NUM2INT(rb_funcall(rb_funcall(rb_cTime, spg_id_local, 6, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec)), spg_id_utc_offset, 0))/SPG_SECONDS_PER_DAY;
      dt = rb_funcall(dtc, spg_id_new, 7, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec), rb_float_new(offset_fraction));
      SPG_DT_ADD_USEC
      if (tz & SPG_APP_UTC) {
        return rb_funcall(dt, spg_id_new_offset, 1, INT2NUM(0));
      } else {
        return dt;
      }
    } else {
      dt = rb_funcall(dtc, spg_id_new, 6, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec));
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

static VALUE spg_fetch_rows_set_cols(VALUE self, VALUE ignore) {
  return Qnil;
}

static VALUE spg__col_value(VALUE self, PGresult *res, long i, long j, VALUE* colconvert
#ifdef SPG_ENCODING
, int enc_index
#endif
) {
  char *v;
  VALUE rv;
  size_t l;

  if(PQgetisnull(res, i, j)) {
    rv = Qnil;
  } else {
    v = PQgetvalue(res, i, j);

    switch(PQftype(res, j)) {
      case 16: /* boolean */
        rv = *v == 't' ? Qtrue : Qfalse;
        break;
      case 17: /* bytea */
        v = (char *)PQunescapeBytea((unsigned char*)v, &l);
        rv = rb_funcall(spg_Blob, spg_id_new, 1, rb_str_new(v, l));
        PQfreemem(v);
        break;
      case 20: /* integer */
      case 21:
      case 23:
      case 26:
        rv = rb_cstr2inum(v, 10);
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
        rv = rb_funcall(spg_BigDecimal, spg_id_new, 1, rb_str_new(v, PQgetlength(res, i, j)));
        break;
      case 1082: /* date */
        rv = spg_date(v, self);
        break;
      case 1083: /* time */
      case 1266:
        rv = spg_time(v);
        break;
      case 1114: /* timestamp */
      case 1184:
        rv = spg_timestamp(v, self);
        break;
      case 18: /* char */
      case 25: /* text */
      case 1043: /* varchar*/
        rv = rb_tainted_str_new(v, PQgetlength(res, i, j));
#ifdef SPG_ENCODING
        rb_enc_associate_index(rv, enc_index);
#endif
        break;
      default:
        rv = rb_tainted_str_new(v, PQgetlength(res, i, j));
#ifdef SPG_ENCODING
        rb_enc_associate_index(rv, enc_index);
#endif
        if (colconvert[j] != Qnil) {
          rv = rb_funcall(colconvert[j], spg_id_call, 1, rv); 
        }
    }
  }
  return rv;
}

static VALUE spg__col_values(VALUE self, VALUE v, VALUE *colsyms, long nfields, PGresult *res, long i, VALUE *colconvert
#ifdef SPG_ENCODING
, int enc_index
#endif
) {
  long j;
  VALUE cur;
  long len = RARRAY_LEN(v);
  VALUE a = rb_ary_new2(len);
  for (j=0; j<len; j++) {
    cur = rb_ary_entry(v, j);
    rb_ary_store(a, j, cur == Qnil ? Qnil : spg__col_value(self, res, i, NUM2LONG(cur), colconvert ENC_INDEX));
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
  VALUE conv_procs = 0;

  nfields = PQnfields(res);

  for(j=0; j<nfields; j++) {
    colsyms[j] = rb_funcall(self, spg_id_output_identifier, 1, rb_str_new2(PQfname(res, j)));
    i = PQftype(res, j);
    switch (i) {
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
      case 1083:
      case 1266:
      case 1114:
      case 1184:
      case 18:
      case 25:
      case 1043:
        colconvert[j] = Qnil;
        break;
      default:
        if (conv_procs == 0) {
          conv_procs = rb_funcall(rb_funcall(self, spg_id_db, 0), spg_id_conversion_procs, 0);
        }
        colconvert[j] = rb_funcall(conv_procs, spg_id_get, 1, INT2NUM(i));
        break;
    }
  }
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
  GetPGresult(rres, res);

#ifdef SPG_ENCODING
  int enc_index;
  enc_index = enc_get_index(rres);
#endif

  ntuples = PQntuples(res);
  nfields = PQnfields(res);
  if (nfields > SPG_MAX_FIELDS) {
    rb_raise(rb_eRangeError, "more than %d columns in query (%ld columns detected)", SPG_MAX_FIELDS, nfields);
  }

  spg_set_column_info(self, res, colsyms, colconvert);

  rb_ivar_set(self, spg_id_columns, rb_ary_new4(nfields, colsyms));

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
          rb_hash_aset(h, colsyms[j], spg__col_value(self, res, i, j, colconvert ENC_INDEX));
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
          rb_yield(spg__col_value(self, res, i, j, colconvert ENC_INDEX));
        } 
      }
      break;
    case SPG_YIELD_COLUMNS:
      /* Multiple columns as an array */
      h = spg__field_ids(pg_value, colsyms, nfields);
      for(i=0; i<ntuples; i++) {
        rb_yield(spg__col_values(self, h, colsyms, nfields, res, i, colconvert ENC_INDEX));
      } 
      break;
    case SPG_YIELD_FIRST:
      /* First column */
      for(i=0; i<ntuples; i++) {
        rb_yield(spg__col_value(self, res, i, 0, colconvert ENC_INDEX));
      } 
      break;
    case SPG_YIELD_ARRAY:
      /* Array of all columns */
      for(i=0; i<ntuples; i++) {
        h = rb_ary_new2(nfields);
        for(j=0; j<nfields; j++) {
          rb_ary_store(h, j, spg__col_value(self, res, i, j, colconvert ENC_INDEX));
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
            rb_hash_aset(h, spg__col_value(self, res, i, k, colconvert ENC_INDEX), spg__col_value(self, res, i, v, colconvert ENC_INDEX));
          } 
        } else {
          VALUE kv, vv, a;
          for(i=0; i<ntuples; i++) {
            kv = spg__col_value(self, res, i, k, colconvert ENC_INDEX);
            vv = spg__col_value(self, res, i, v, colconvert ENC_INDEX);
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
            rb_hash_aset(h, spg__col_values(self, k, colsyms, nfields, res, i, colconvert ENC_INDEX), spg__col_value(self, res, i, v, colconvert ENC_INDEX));
          } 
        } else {
          VALUE kv, vv, a;
          for(i=0; i<ntuples; i++) {
            kv = spg__col_values(self, k, colsyms, nfields, res, i, colconvert ENC_INDEX);
            vv = spg__col_value(self, res, i, v, colconvert ENC_INDEX);
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
            rb_hash_aset(h, spg__col_value(self, res, i, k, colconvert ENC_INDEX), spg__col_values(self, v, colsyms, nfields, res, i, colconvert ENC_INDEX));
          } 
        } else {
          VALUE kv, vv, a;
          for(i=0; i<ntuples; i++) {
            kv = spg__col_value(self, res, i, k, colconvert ENC_INDEX);
            vv = spg__col_values(self, v, colsyms, nfields, res, i, colconvert ENC_INDEX);
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
            rb_hash_aset(h, spg__col_values(self, k, colsyms, nfields, res, i, colconvert ENC_INDEX), spg__col_values(self, v, colsyms, nfields, res, i, colconvert ENC_INDEX));
          } 
        } else {
          VALUE kv, vv, a;
          for(i=0; i<ntuples; i++) {
            kv = spg__col_values(self, k, colsyms, nfields, res, i, colconvert ENC_INDEX);
            vv = spg__col_values(self, v, colsyms, nfields, res, i, colconvert ENC_INDEX);
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
          rb_hash_aset(h, colsyms[j], spg__col_value(self, res, i, j, colconvert ENC_INDEX));
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
  GetPGconn(self, conn);
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
  GetPGconn(rconn, conn);

  rres = rb_funcall(rconn, spg_id_get_result, 0);
  rb_funcall(rres, spg_id_check, 0);
  GetPGresult(rres, res);

#ifdef SPG_ENCODING
  int enc_index;
  enc_index = enc_get_index(rres);
#endif

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

  rb_ivar_set(self, spg_id_columns, rb_ary_new4(nfields, colsyms));

  while (PQntuples(res) != 0) {
    h = rb_hash_new();
    for(j=0; j<nfields; j++) {
      rb_hash_aset(h, colsyms[j], spg__col_value(self, res, 0, j, colconvert ENC_INDEX));
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
    rb_funcall(rres, spg_id_check, 0);
    GetPGresult(rres, res);
  }
  rb_funcall(rres, spg_id_clear, 0);

  return self;
}

static VALUE spg__flush_results(VALUE rconn) {
  PGconn *conn;
  PGresult *res;
  VALUE error = 0;
  GetPGconn(rconn, conn);

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
  spg_id_new = rb_intern("new");
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

  spg_Sequel = rb_funcall(rb_cObject, cg, 1, rb_str_new2("Sequel"));
  spg_Blob = rb_funcall(rb_funcall(spg_Sequel, cg, 1, rb_str_new2("SQL")), cg, 1, rb_str_new2("Blob")); 
  spg_SQLTime= rb_funcall(spg_Sequel, cg, 1, rb_str_new2("SQLTime")); 
  spg_BigDecimal = rb_funcall(rb_cObject, cg, 1, rb_str_new2("BigDecimal")); 
  spg_Date = rb_funcall(rb_cObject, cg, 1, rb_str_new2("Date")); 
  spg_Postgres = rb_funcall(spg_Sequel, cg, 1, rb_str_new2("Postgres"));
  spg_PGError = rb_funcall(rb_cObject, cg, 1, rb_str_new2("PGError"));

  spg_nan = rb_eval_string("0.0/0.0");
  spg_pos_inf = rb_eval_string("1.0/0.0");
  spg_neg_inf = rb_eval_string("-1.0/0.0");

  rb_global_variable(&spg_Sequel);
  rb_global_variable(&spg_Blob);
  rb_global_variable(&spg_BigDecimal);
  rb_global_variable(&spg_Date);
  rb_global_variable(&spg_SQLTime);
  rb_global_variable(&spg_PGError);
  rb_global_variable(&spg_nan);
  rb_global_variable(&spg_pos_inf);
  rb_global_variable(&spg_neg_inf);

  /* Check for 1.8-1.9.2 stdlib date that needs Rational for usec accuracy */
  if (rb_eval_string("Date.today.instance_variable_get(:@ajd)") != Qnil) {
    spg_id_Rational = rb_intern("Rational");
  }
  if (rb_eval_string("defined?(PG::TypeMapAllStrings)") != Qnil) {
    unwrap_structs = 1;
  }

  c = rb_funcall(spg_Postgres, cg, 1, rb_str_new2("Dataset"));
  rb_define_private_method(c, "yield_hash_rows", spg_yield_hash_rows, 2);
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
}
