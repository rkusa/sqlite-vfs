extern int sqlite3_sync_count;

void sqlite3_inc_sync_count() {
  sqlite3_sync_count++;
}

extern int sqlite3_fullsync_count;

void sqlite3_inc_fullsync_count() {
  sqlite3_fullsync_count++;
}

extern int sqlite3_current_time;

void sqlite3_set_current_time(int current_time) {
  sqlite3_current_time = current_time;
}

int sqlite3_get_current_time() {
  return sqlite3_current_time;
}

extern int sqlite3_diskfull_pending;

void sqlite3_dec_diskfull_pending() {
  sqlite3_diskfull_pending--;
}

int sqlite3_get_diskfull_pending() {
  return sqlite3_diskfull_pending;
}

extern int sqlite3_diskfull;

void sqlite3_set_diskfull() {
  sqlite3_diskfull = 1;
}

extern int sqlite3_open_file_count;

void sqlite3_inc_open_file_count() {
  sqlite3_open_file_count++;
}

void sqlite3_dec_open_file_count() {
  sqlite3_open_file_count--;
}

extern int sqlite3_io_error_pending;

int sqlite3_dec_io_error_pending() {
  return sqlite3_io_error_pending--;
}

extern int sqlite3_io_error_persist;

int sqlite3_get_io_error_persist() {
  return sqlite3_io_error_persist;
}

extern int sqlite3_io_error_hit;

int sqlite3_get_io_error_hit() {
  return sqlite3_io_error_hit;
}

void sqlite3_inc_io_error_hit() {
  sqlite3_io_error_hit++;
}

void sqlite3_set_io_error_hit(int hit) {
  sqlite3_io_error_hit = hit;
}

extern int sqlite3_io_error_benign;

int sqlite3_get_io_error_benign() {
  return sqlite3_io_error_benign;
}

// void sqlite3_set_io_error_benign(int benign) {
//   sqlite3_io_error_benign = benign;
// }

extern int sqlite3_io_error_hardhit;

int sqlite3_inc_io_error_hardhit() {
  sqlite3_io_error_hardhit++;
}
