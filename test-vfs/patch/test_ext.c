extern int sqlite3_sync_count, sqlite3_fullsync_count;

void sqlite3_inc_sync_count() {
  sqlite3_sync_count++;
}

void sqlite3_inc_fullsync_count() {
  sqlite3_fullsync_count++;
}
