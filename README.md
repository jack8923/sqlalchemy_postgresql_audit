The default library creates audit-table and corresponding for every table present in the database while this library creates two table irrespective of the no. of tables in the database i.e -
  i. audit_transaction_table : type of transaction
  ii. audit_data_table : row entry of transacted row

Steps
 i. Clone the repo.
 ii. import sqlalchemy_postgresql_audit
 iii. sqlalchemy_postgresql_audit.enable()
 iv. sqlalchemy_postgresql_audit.install_audit_triggers(meta)

Note : step iii and iv should be implemented as for the default library.
