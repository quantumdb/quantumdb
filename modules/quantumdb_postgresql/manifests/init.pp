class quantumdb_postgresql($version = "9.6"){

	$listen = "*"
	$shmMax = "1142308864"

	file_line { 'sysCtlShmmax':
		match   => '^kernel\.shmmax.*$',
		line    => "kernel.shmmax=$shmMax",
		ensure  => present,
		path    => '/etc/sysctl.conf',
		notify  => Exec['loadSysCtl']
	}

	exec { 'loadSysCtl':
		path        => '/sbin',
		command     => "sysctl -p",
		refreshonly => true
	}

	class { 'postgresql::globals':
		encoding            => 'UTF-8',
		locale              => 'en_US.UTF-8',
		manage_package_repo => true,
		version             => $version,
	}
	->
	class { 'postgresql::server':
		listen_addresses => $listen,
		ipv4acls         => ['host all all 0.0.0.0/0 md5','host all all ::1/0 md5'],
		require          => File_Line['sysCtlShmmax']
	}

	postgresql::server::config_entry {
		'max_connections':      value => 112;
		'shared_buffers':       value => "512MB";
		'effective_cache_size': value => "1500MB";
		'work_mem':             value => "4MB";
		'random_page_cost':     value => 1.0;
	}

	firewall { '103 postgreSQL':
		dport   => [5432],
		proto   => tcp,
		action  => accept,
		iniface => 'eth1'
	}

}
