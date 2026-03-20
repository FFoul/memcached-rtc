/* associative array */
void assoc_init(const int hashpower_init);
#ifdef RTC_BENCHMARK
item *assoc_find(uint64_t key, const uint32_t hv);
void assoc_delete(uint64_t key, const uint32_t hv);
#else
item *assoc_find(const char *key, const size_t nkey, const uint32_t hv);
#endif
int assoc_insert(item *item, const uint32_t hv);
void do_assoc_move_next_bucket(void);
int start_assoc_maintenance_thread(void);
void stop_assoc_maintenance_thread(void);
