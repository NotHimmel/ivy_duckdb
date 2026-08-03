#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_guc.hpp"

#include "pgduckdb/pgduckdb_duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
}

#include "pgduckdb/pgduckdb_background_worker.hpp"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"

extern "C" {

#ifdef PG_MODULE_MAGIC_EXT
#ifndef PG_DUCKDB_VERSION
// Should always be defined via build system, but keep a fallback here for
// static analysis tools etc.
#define PG_DUCKDB_VERSION "unknown"
#endif
PG_MODULE_MAGIC_EXT(.name = "pg_duckdb", .version = PG_DUCKDB_VERSION);
#endif

} // extern "C"

/*
 * Build provenance, surfaced through the duckdb.build_info GUC.
 *
 * Why per-translation-unit timestamps: a stale object file linked into an
 * otherwise fresh .so produces a binary whose behaviour silently disagrees
 * with the source tree (we shipped exactly that: pgduckdb_types.o from one
 * date, the rest from two other dates, relinked on a fourth). A single
 * "version" string cannot catch it, so each TU that carries behaviour we care
 * about reports its own compile time. types= drifting behind core= means the
 * type mapping in this binary is NOT what the source says.
 */
#ifndef PG_DUCKDB_VERSION
/*
 * Fallback: the Makefile passes -DPG_DUCKDB_VERSION only as a target-specific
 * flag on src/pgduckdb.o, which PGXS may expand too late to take effect.
 * "unknown" here is not fatal — the core=/types= timestamps below are what
 * actually catch a mixed build.
 */
#define PG_DUCKDB_VERSION "unknown"
#endif

namespace pgduckdb {
const char *
BuildRevision() {
	return PG_DUCKDB_VERSION;
}
const char *
CoreBuildTimestamp() {
	return __DATE__ " " __TIME__;
}
} // namespace pgduckdb

extern "C" {

#ifdef PG_MODULE_MAGIC_EXT
#else
PG_MODULE_MAGIC;
#endif

void
_PG_init(void) {
	if (!process_shared_preload_libraries_in_progress) {
		ereport(ERROR, (errmsg("pg_duckdb needs to be loaded via shared_preload_libraries"),
		                errhint("Add pg_duckdb to shared_preload_libraries.")));
	}

	pgduckdb::InitGUC();
	pgduckdb::InitGUCHooks();
	DuckdbInitHooks();
	DuckdbInitNode();
	pgduckdb::InitBackgroundWorkersShmem();
	pgduckdb::RegisterDuckdbXactCallback();
}
} // extern "C"
