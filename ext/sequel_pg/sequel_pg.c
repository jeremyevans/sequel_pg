#include <string.h>
#include <stdio.h>
#include <math.h>
#include <ruby.h>
#include <libpq-fe.h>

#define SPG_MAX_FIELDS 256
#define SPG_MILLISECONDS_PER_DAY 86400000000.0
#define SPG_MINUTES_PER_DAY 1440.0
#define SPG_SECONDS_PER_DAY 86400.0

#define SPG_DT_ADD_USEC if (usec != 0) { dt = rb_funcall(dt, spg_id_op_plus, 1, rb_float_new(usec/SPG_MILLISECONDS_PER_DAY)); }

#define SPG_NO_TZ 0
#define SPG_DB_LOCAL 1
#define SPG_DB_UTC 2
#define SPG_APP_LOCAL 4
#define SPG_APP_UTC 8

static VALUE spg_Sequel;
static VALUE spg_Blob;
static VALUE spg_BigDecimal;
static VALUE spg_Date;

static VALUE spg_sym_utc;
static VALUE spg_sym_local;

static ID spg_id_new;
static ID spg_id_local;
static ID spg_id_year;
static ID spg_id_month;
static ID spg_id_day;
static ID spg_id_columns;
static ID spg_id_output_identifier;
static ID spg_id_datetime_class;
static ID spg_id_application_timezone;
static ID spg_id_database_timezone;
static ID spg_id_op_plus;
static ID spg_id_utc;
static ID spg_id_utc_offset;
static ID spg_id_localtime;
static ID spg_id_new_offset;

static VALUE spg_time(const char *s) {
  VALUE now;
  int hour, minute, second, tokens;
  char subsec[7];
  int usec = 0;

  tokens = sscanf(s, "%2d:%2d:%2d.%s", &hour, &minute, &second, subsec);
  if(tokens == 4) {
    usec = atoi(subsec);
    usec *= (int) pow(10, (6 - strlen(subsec)));
  } else if(tokens < 3) {
    rb_raise(rb_eArgError, "unexpected time format");
  }

  now = rb_funcall(rb_cTime, spg_id_new, 0);
  return rb_funcall(rb_cTime, spg_id_local, 6, rb_funcall(now, spg_id_year, 0), rb_funcall(now, spg_id_month, 0), rb_funcall(now, spg_id_day, 0), INT2NUM(hour), INT2NUM(minute), INT2NUM(second), INT2NUM(usec));
}

static VALUE spg_date(const char *s) {
  int year, month, day;

  if(3 != sscanf(s, "%d-%2d-%2d", &year, &month, &day)) {
    rb_raise(rb_eArgError, "unexpected date format");
  }

  return rb_funcall(spg_Date, spg_id_new, 3, INT2NUM(year), INT2NUM(month), INT2NUM(day));
}

static VALUE spg_timestamp(const char *s) {
  VALUE dtc, dt, rtz;
  int tz = SPG_NO_TZ;
  int year, month, day, hour, min, sec, usec, tokens, pos, utc_offset;
  int check_offset = 0;
  int offset_hour = 0;
  int offset_minute = 0;
  int offset_seconds = 0;
  double offset_fraction = 0.0;
  char subsec[7];

  if (0 != strchr(s, '.')) {
    tokens = sscanf(s, "%d-%2d-%2d %2d:%2d:%2d.%s%n", &year, &month, &day, &hour, &min, &sec, subsec, &pos);
    if (tokens == 8) {
      check_offset = 1;
    }
    if(tokens != 7) {
      rb_raise(rb_eArgError, "unexpected datetime format");
    }
    usec = atoi(subsec);
    usec *= (int) pow(10, (6 - strlen(subsec)));
  } else {
    tokens = sscanf(s, "%d-%2d-%2d %2d:%2d:%2d%n", &year, &month, &day, &hour, &min, &sec, &pos);
    if (tokens == 3) {
      hour = 0;
      min = 0;
      sec = 0;
    } else if (tokens == 7) {
      check_offset = 1;
    } else if (tokens != 6) {
      rb_raise(rb_eArgError, "unexpected datetime format");
    }
    usec = 0;
  }

  if (check_offset) {
    if(sscanf(s + pos, "%3d:%2d", &offset_hour, &offset_minute) == 0) {
      /* No offset found */
      check_offset = 0;
    }
  }

  /* Get values of datetime_class, database_timezone, and application_timezone */
  dtc = rb_funcall(spg_Sequel, spg_id_datetime_class, 0);
  rtz = rb_funcall(spg_Sequel, spg_id_database_timezone, 0);
  if (rtz == spg_sym_local) {
    tz += SPG_DB_LOCAL;
  } else if (rtz == spg_sym_utc) {
    tz += SPG_DB_UTC;
  }
  rtz = rb_funcall(spg_Sequel, spg_id_application_timezone, 0);
  if (rtz == spg_sym_local) {
    tz += SPG_APP_LOCAL;
  } else if (rtz == spg_sym_utc) {
    tz += SPG_APP_UTC;
  }

  if (dtc == rb_cTime) {
    if (check_offset) {
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
    if (check_offset) {
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

static VALUE spg_fetch_rows_set_cols(VALUE self, VALUE rres) {
  return self;
}

static VALUE spg_yield_hash_rows(VALUE self, VALUE rres, VALUE ignore) {
  PGresult *res;
  VALUE colsyms[SPG_MAX_FIELDS];
  long ntuples;
  long nfields;
  long i;
  long j;
  VALUE h, rv;
  size_t l;
  char * v;

  Data_Get_Struct(rres, PGresult, res);
  ntuples = PQntuples(res);
  nfields = PQnfields(res);
  if (nfields > SPG_MAX_FIELDS) {
    rb_raise(rb_eRangeError, "more than %d columns in query", SPG_MAX_FIELDS);
  }

  for(j=0; j<nfields; j++) {
    colsyms[j] = rb_funcall(self, spg_id_output_identifier, 1, rb_str_new2(PQfname(res, j)));
  }
  rb_ivar_set(self, spg_id_columns, rb_ary_new4(nfields, colsyms));

  for(i=0; i<ntuples; i++) {
    h = rb_hash_new();
    for(j=0; j<nfields; j++) {
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
          case 22:
          case 23:
          case 26:
            rv = rb_cstr2inum(v, 10);
            break;
          case 700: /* float */
          case 701:
            rv = rb_float_new(rb_cstr_to_dbl(v, Qfalse));
            break;
          case 790: /* numeric */
          case 1700:
            rv = rb_funcall(spg_BigDecimal, spg_id_new, 1, rb_str_new(v, PQgetlength(res, i, j)));
            break;
          case 1082: /* date */
            rv = spg_date(v);
            break;
          case 1083: /* time */
          case 1266:
            rv = spg_time(v);
            break;
          case 1114: /* timestamp */
          case 1184:
            rv = spg_timestamp(v);
            break;
          default:
            rv = rb_tainted_str_new(v, PQgetlength(res, i, j));
        }
      }
      rb_hash_aset(h, colsyms[j], rv);
    }
    rb_yield(h);
  }
  
  return self;
}

void Init_sequel_pg(void) {
  VALUE c;
  ID cg;
  cg = rb_intern("const_get");
  spg_id_new = rb_intern("new");
  spg_id_local = rb_intern("local");
  spg_id_year = rb_intern("year");
  spg_id_month = rb_intern("month");
  spg_id_day = rb_intern("day");
  spg_id_columns = rb_intern("@columns");
  spg_id_output_identifier = rb_intern("output_identifier");
  spg_id_datetime_class = rb_intern("datetime_class");
  spg_id_application_timezone = rb_intern("application_timezone");
  spg_id_database_timezone = rb_intern("database_timezone");
  spg_id_op_plus = rb_intern("+");
  spg_id_utc = rb_intern("utc");
  spg_id_utc_offset = rb_intern("utc_offset");
  spg_id_localtime = rb_intern("localtime");
  spg_id_new_offset = rb_intern("new_offset");

  spg_sym_utc = ID2SYM(rb_intern("utc"));
  spg_sym_local = ID2SYM(rb_intern("local"));

  spg_Sequel = rb_funcall(rb_cObject, cg, 1, rb_str_new2("Sequel"));
  spg_Blob = rb_funcall(rb_funcall(spg_Sequel, cg, 1, rb_str_new2("SQL")), cg, 1, rb_str_new2("Blob")); 
  spg_BigDecimal = rb_funcall(rb_cObject, cg, 1, rb_str_new2("BigDecimal")); 
  spg_Date = rb_funcall(rb_cObject, cg, 1, rb_str_new2("Date")); 

  rb_global_variable(&spg_Sequel);
  rb_global_variable(&spg_Blob);
  rb_global_variable(&spg_BigDecimal);
  rb_global_variable(&spg_Date);

  c = rb_funcall(rb_funcall(spg_Sequel, cg, 1, rb_str_new2("Postgres")), cg, 1, rb_str_new2("Dataset"));
  rb_define_private_method(c, "yield_hash_rows", spg_yield_hash_rows, 2);
  rb_define_private_method(c, "fetch_rows_set_cols", spg_fetch_rows_set_cols, 1);
}
