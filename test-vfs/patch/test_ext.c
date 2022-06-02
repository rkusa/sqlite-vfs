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
