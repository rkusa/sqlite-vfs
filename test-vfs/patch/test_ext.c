extern int sqlite3_sync_count, sqlite3_fullsync_count, sqlite3_current_time;

void sqlite3_inc_sync_count() {
  sqlite3_sync_count++;
}

void sqlite3_inc_fullsync_count() {
  sqlite3_fullsync_count++;
}

void sqlite3_set_current_time(int current_time) {
  sqlite3_current_time = current_time;
}

int sqlite3_get_current_time() {
  return sqlite3_current_time;
}
