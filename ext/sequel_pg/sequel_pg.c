#include <string.h>
#include <stdio.h>
#include <math.h>
#include <ruby.h>
#include <libpq-fe.h>

#define SPG_MAX_FIELDS 256
#define SPG_MILLISECONDS_PER_DAY 86400000000.0

static VALUE spg_Sequel;
static VALUE spg_Blob;
static VALUE spg_BigDecimal;
static VALUE spg_Date;

static ID spg_id_new;
static ID spg_id_local;
static ID spg_id_year;
static ID spg_id_month;
static ID spg_id_day;
static ID spg_id_columns;
static ID spg_id_output_identifier;
static ID spg_id_datetime_class;
static ID spg_id_op_plus;

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
  VALUE dtc;
  int year, month, day, hour, min, sec, usec, tokens;
  char subsec[7];

  if (0 != strchr(s, '.')) {
    tokens = sscanf(s, "%d-%2d-%2d %2d:%2d:%2d.%s", &year, &month, &day, &hour, &min, &sec, subsec);
    if(tokens != 7) {
      rb_raise(rb_eArgError, "unexpected datetime format");
    }
    usec = atoi(subsec);
    usec *= (int) pow(10, (6 - strlen(subsec)));
  } else {
    tokens = sscanf(s, "%d-%2d-%2d %2d:%2d:%2d", &year, &month, &day, &hour, &min, &sec);
    if (tokens == 3) {
      hour = 0;
      min = 0;
      sec = 0;
    } else if (tokens != 6) {
      rb_raise(rb_eArgError, "unexpected datetime format");
    }
    usec = 0;
  }

  dtc = rb_funcall(spg_Sequel, spg_id_datetime_class, 0);
  if (dtc == rb_cTime) {
    return rb_funcall(rb_cTime, spg_id_local, 7, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec), INT2NUM(usec));
  } else {
    dtc = rb_funcall(dtc, spg_id_new, 6, INT2NUM(year), INT2NUM(month), INT2NUM(day), INT2NUM(hour), INT2NUM(min), INT2NUM(sec));
    if (usec != 0) {
      dtc = rb_funcall(dtc, spg_id_op_plus, 1, rb_float_new(usec/SPG_MILLISECONDS_PER_DAY));
    }
    return dtc;
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
  spg_id_op_plus = rb_intern("+");

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
