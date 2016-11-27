include stdlib

include quantumdb_postgresql

postgresql::server::db{ ["quantumdb"]:
	user     => "quantumdb",
	password => "quantumdb",
	require  => Class[quantumdb_postgresql]
}
~>
exec { "set_schema_owner_quantumdb":
	require     => Class['postgresql::server'],
	command     => "sudo -u postgres psql quantumdb -c 'ALTER SCHEMA public OWNER TO quantumdb'",
	path        => "/usr/bin/:/usr/bin/psql",
	refreshonly => true
}
~>
exec { "make_super_user":
	require     => Class['postgresql::server'],
	command     => "sudo -u postgres psql quantumdb -c 'ALTER USER quantumdb WITH SUPERUSER;'",
	path        => "/usr/bin/:/usr/bin/psql",
	refreshonly => true
}
