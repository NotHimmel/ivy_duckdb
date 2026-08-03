# Makefile for regression test

ROOT_DIR = ../..

include $(ROOT_DIR)/Makefile.global

# Make sure foreign_data_wrapper tests fail as expected
unexport MOTHERDUCK_TOKEN
unexport motherduck_token

check-regression-duckdb:
	TEST_DIR=$(CURDIR) $(pg_regress_installcheck) \
	--encoding=UTF8 \
	--no-locale \
	--temp-instance=./tmp_check \
	--temp-config regression.conf \
	--load-extension=pg_duckdb \
	$(if $(TEST),$(TEST),--schedule=schedule)

# IvorySQL-specific: Oracle types x pg_duckdb execution paths.
#
# --temp-instance is not usable: the cluster pg_regress initdb's itself is in
# native PG mode, SET ivorysql.compatible_mode = oracle is rejected outright,
# and the whole suite is useless. So this is an installcheck against an
# ALREADY-RUNNING IvorySQL instance.
#
# Prerequisites (violations fail loudly in section 0 of the test, no false
# green):
#   1. instance initdb'ed in Oracle mode (IvorySQL initdb default)
#   2. shared_preload_libraries = 'liboracle_parser,ivorysql_ora,pg_duckdb'
#   3. PGHOST / PGPORT / PGUSER point at that instance; the role can create
#      databases and extensions
# Remaining GUCs (force_execution, unsafe_allow_execution_inside_functions)
# are SET by the test itself; no server config changes needed.
#
# Usage:
#   PGPORT=1521 PGDATABASE=ivorysql make -C test/regression check-regression-ivorysql
check-regression-ivorysql:
	TEST_DIR=$(CURDIR) $(pg_regress_installcheck) \
	--encoding=UTF8 \
	--no-locale \
	--load-extension=pg_duckdb \
	$(if $(TEST),$(TEST),--schedule=schedule_ivorysql)

clean-regression:
	rm -fr $(CURDIR)/tmp_check
	rm -fr $(CURDIR)/log
	rm -fr $(CURDIR)/results
	rm -f  $(CURDIR)/regression.diffs
	rm -f  $(CURDIR)/regression.out
