IF XACT_STATE() = 1 COMMIT;

create table dbresult_migration_test 
(
	analysis_id int,
	test_message varchar(50)
)
with (CLUSTERED COLUMNSTORE INDEX, distribution=hash(analysis_id));


