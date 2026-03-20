/* See items.c */
uint64_t get_cas_id(void);

/*@null@*/
#ifdef RTC_BENCHMARK
item *do_item_alloc(uint64_t key, const int flags, const rel_time_t exptime,
                    const int nbytes);
bool item_size_ok(const int flags, const int nbytes);
item *do_item_get(uint64_t key, const uint32_t hv);
item *do_item_touch(uint64_t key, uint32_t exptime, const uint32_t hv);
#else
item *do_item_alloc(char *key, const size_t nkey, const int flags, const rel_time_t exptime, const int nbytes);
bool item_size_ok(const size_t nkey, const int flags, const int nbytes);
item *do_item_get(const char *key, const size_t nkey, const uint32_t hv);
item *do_item_touch(const char *key, const size_t nkey, uint32_t exptime, const uint32_t hv);
#endif
void item_free(item *it);

int  do_item_link(item *it, const uint32_t hv);     /** may fail if transgresses limits */
void do_item_unlink(item *it, const uint32_t hv);
void do_item_unlink_nolock(item *it, const uint32_t hv);
void do_item_remove(item *it);
void do_item_update(item *it);   /** update LRU time to current and reposition */
int  do_item_replace(item *it, item *new_it, const uint32_t hv);

/*@null@*/
char *do_item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes);
void do_item_stats(ADD_STAT add_stats, void *c);
/*@null@*/
void do_item_stats_sizes(ADD_STAT add_stats, void *c);
void do_item_flush_expired(void);
void item_stats_reset(void);
extern pthread_mutex_t cache_lock;
