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

# IvorySQL 专用：Oracle 类型 x pg_duckdb 执行路径。
#
# 不用 --temp-instance：pg_regress 自己跑的 initdb 出来是 native PG 模式的簇，
# SET ivorysql.compatible_mode = oracle 会被直接拒绝，整套用例全废。
# 因此对一个**已经跑起来的** IvorySQL 实例做 installcheck。
#
# 前置条件（不满足会在用例第 0 节立刻报错，不会静默跑出假绿）：
#   1. 实例以 Oracle 模式 initdb（IvorySQL initdb 默认就是 oracle）
#   2. shared_preload_libraries = 'liboracle_parser,ivorysql_ora,pg_duckdb'
#   3. PGHOST / PGPORT / PGUSER 指向该实例，当前角色能建库建扩展
# 其余 GUC（force_execution、unsafe_allow_execution_inside_functions）
# 由用例自己 SET，不需要改服务端配置。
#
# 用法：
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
