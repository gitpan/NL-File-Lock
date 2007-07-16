#!/usr/bin/perl
# NL::File::Lock - Extended file locking (based on lockfiles)
# (C) 2007 Nickolay Kovalev, http://resume.nickola.ru
# E-mail: nickola_code@nickola.ru

package NL::File::Lock;
use strict;
use warnings; # For debugging only, will be removed at compact release | NL_CODE: RM_LINE

our $VERSION = '0.2';
sub LOCK_SH() {1} # multi-lock
sub LOCK_EX() {2} # mono-lock
sub LOCK_NB() {4} # don't wait lock result
sub LOCK_UN() {8} # unlock

$NL::File::Lock::DATA = {
	'CONST' => {
		'LOCK_POSTFIX' => '.lck',
		'dir_splitter' => '/',
		'dir_splitters_extra' => [],
		'dir_for_locks' => ''
	},
	'LOCKED_FILES' => {}
};
sub _path_get_file {
	my ($str) = @_;

	foreach my $spl ( ($NL::File::Lock::DATA->{'dir_splitter'}, @{ $NL::File::Lock::DATA->{'dir_splitters_extra'} }) ) {
		$spl =~ s/\\/\\\\/g; $spl =~ s/\:/\\\:/g;
		$str =~ s/^.*$spl([^$spl]{0,})$/$1/;
	}
	return $str;
}
sub _path_dir_chomp {
	my ($ref_str) = @_;

	foreach my $spl ( ($NL::File::Lock::DATA->{'dir_splitter'}, @{ $NL::File::Lock::DATA->{'dir_splitters_extra'} }) ) {
		$spl =~ s/\\/\\\\/g; $spl =~ s/\:/\\\:/g;
		${ $ref_str } =~ s/[$spl]{1,}$//;
	}
}
sub init {
	my ($dir_for_locks) = @_;

	if ($^O eq 'MacOS') { $NL::File::Lock::DATA->{'dir_splitter'} = ':'; }
	elsif ($^O eq 'MSWin32') {
		$NL::File::Lock::DATA->{'dir_splitter'} = '/';
		$NL::File::Lock::DATA->{'dir_splitters_extra'} = ['\\'];
	}
	if (defined $dir_for_locks && $dir_for_locks ne '') {
		&_path_dir_chomp(\$dir_for_locks);
		if ($dir_for_locks ne '' && -d $dir_for_locks) {
			$NL::File::Lock::DATA->{'CONST'}->{'dir_for_locks'} = $dir_for_locks;
		}
	}

}
sub _make_lock_file_name {
	my ($file_name) = @_;

	if ($NL::File::Lock::DATA->{'CONST'}->{'dir_for_locks'} ne '') {
		my $fn = &_path_get_file($file_name);
		if ($fn ne '') {
			return  $NL::File::Lock::DATA->{'CONST'}->{'dir_for_locks'}.$NL::File::Lock::DATA->{'dir_splitter'}.$fn.$NL::File::Lock::DATA->{'CONST'}->{'LOCK_POSTFIX'};
		}
	}
	return $file_name.$NL::File::Lock::DATA->{'CONST'}->{'LOCK_POSTFIX'};
}

sub lock_read  { my ($file_name, $in_ref_hash_EXT) = @_; $in_ref_hash_EXT = {} if (!defined $in_ref_hash_EXT); return &lf_lock($file_name, &LOCK_SH(), $in_ref_hash_EXT); }
sub lock_write { my ($file_name, $in_ref_hash_EXT) = @_; $in_ref_hash_EXT = {} if (!defined $in_ref_hash_EXT); return &lf_lock($file_name, &LOCK_EX(), $in_ref_hash_EXT); }
sub lf_lock {
	my ($file_name, $lock_type, $in_ref_hash_EXT) = @_;
	$lock_type = &LOCK_EX() if (!defined $lock_type || $lock_type <= 0);
	$in_ref_hash_EXT = {} if (!defined $in_ref_hash_EXT || ref $in_ref_hash_EXT ne 'HASH');

	my $lock_file_name = '';
	my ($time_stop, $time_sleep) = (0, 0);
	if (defined $in_ref_hash_EXT->{'timeout'}) {
		$time_sleep = (defined $in_ref_hash_EXT->{'time_sleep'} && $in_ref_hash_EXT->{'time_sleep'} > 0) ? $in_ref_hash_EXT->{'time_sleep'} : 0;
		$time_stop = time() + $in_ref_hash_EXT->{'timeout'};
	}
	if (defined $NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name}) {
		if ($NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name}->{'status'} eq 'locked') { return 2; } # already locked
		else {
			if (&_lf_lock_MAKE_LOCK($NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name}->{'lock_handle'}, $lock_type, $time_stop, $time_sleep)) {
				# Locked
				$NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name}->{'status'} ='locked';
				return 1;
			}
			else {  return 0; }
		}
	}
	else { $lock_file_name = &_make_lock_file_name($file_name); }

	do {
		if (open(LFH, ">>$lock_file_name")) {
			if (&_lf_lock_MAKE_LOCK(\*LFH, $lock_type, $time_stop, $time_sleep)) {
				# Locked
				$NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name} = { 'status' => 'locked', 'lock_file' => $lock_file_name, 'lock_handle' => \*LFH };
				return 1;
			}
			else {
				close(LFH);
				return 0;
			}
		}
		else { sleep($time_sleep) if ($time_sleep > 0); }
	} while (time() < $time_stop);
	return 0;
}
sub _lf_lock_MAKE_LOCK {
	my ($lock_file_handle, $lock_type, $time_stop, $time_sleep) = @_;

	do {
		if (flock($lock_file_handle, $lock_type | &LOCK_NB())) { return 1; }
		else { sleep($time_sleep) if ($time_sleep > 0); }

	} while (time() < $time_stop);
	return 0;
}
sub unlock {
	my ($file_name, $not_unlink) = @_;
	$not_unlink = 0 if (!defined $not_unlink);

	if (defined $NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name})
	{
		if ($NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name}->{'status'} eq 'locked') {
			flock($NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name}->{'lock_handle'}, &LOCK_UN());
		}
		close $NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name}->{'lock_handle'};
		unlink $NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name}->{'lock_file'} if (!$not_unlink); # If file is opened it will not be removed on some OS
		delete $NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name};
		return 1;
	}
	return 0;
}
sub unlock_not_unlink {
	my ($file_name) = @_;
	return &unlock($file_name, 1);
}

# DO NOT USE 'unlock_not_close' - USE 'unlock_not_unlink'
# 'unlock_not_close' is not good because, proccess A can make 'unlock_not_close' and proccess B
# can remove lock file on some OS then, when proccess A will make lock again via FILE_HANDLE - can be error
sub unlock_not_close {
	my ($file_name) = @_;

	if (defined $NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name})
	{
		if ($NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name}->{'status'} eq 'locked') {

			if ($] < 5.004) {
				# Fix for old Perl
				my $old_fh = select($NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name}->{'lock_handle'});
				local $|=1;               # Enable commands bufferization
				local $\ = '';            # Make empty splitter of output records
				print '';                 # Call buffer cleaning
				select($old_fh);          # Restore old HANDLER
			}

			flock($NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name}->{'lock_handle'}, &LOCK_UN()); # LOCK_UN = 8
			$NL::File::Lock::DATA->{'LOCKED_FILES'}->{$file_name}->{'status'} = 'unlocked';
			return 1;
		}
	}
	return 0;
}
sub END
{
	foreach (keys %{ $NL::File::Lock::DATA->{'LOCKED_FILES'} }) { &unlock($_); }
}

# START BLOCK for debugging only, will be removed at compact release | NL_CODE: RM_BLOCK
# TESTING
# &init('./_tmp/'); &TEST(shift);
sub TEST {
	my $test_in = shift || 0;
	if ($test_in == 0) {
		if (&NL::File::Lock::lock_write('file', { 'timeout' => 10, 'time_sleep' => 0.1 } )) {
			print "+Locked EX (write)...\n";
			sleep(5);
			&NL::File::Lock::unlock_not_unlink('file');
			print "-UnLocked for some time...\n";
			sleep(5);
			if (&NL::File::Lock::lock_write('file', { 'timeout' => 10, 'time_sleep' => 0.1 } )) {
				print "+Locked EX (write)...\n";
				sleep(5);
				&NL::File::Lock::unlock('file');
				print "-UnLocked forever...\n";
				sleep(5);
			}
			else { print "Unable to lock EX (write) again...\n"; }
		}
		else {
			print "Unable to lock EX (write)...\n";
		}
	}
	elsif ($test_in == 1) {
		if (&NL::File::Lock::lock_write('file', { 'timeout' => 10, 'time_sleep' => 0.1 } )) {
			print "+Locked EX (write)...\n";
			sleep(2);
			&NL::File::Lock::unlock('file');
			print "-UnLocked forever...\n";
		}
		else {
			print "Unable to lock EX (write)...\n";
		}
	}
	elsif ($test_in == 2) {
		if (&NL::File::Lock::lock_read('file', { 'timeout' => 10, 'time_sleep' => 0.1 } )) {
			print "+Locked SH (read)...\n";
			sleep(5);
			&NL::File::Lock::unlock_not_close('file');
			print "-UnLocked for some time...\n";
			sleep(5);
			if (&NL::File::Lock::lock_read('file', { 'timeout' => 10, 'time_sleep' => 0.1 } )) {
				print "+Locked SH (read)...\n";
				sleep(5);
				&NL::File::Lock::unlock('file');
				print "-UnLocked forever...\n";
				sleep(5);
			}
			else { print "Unable to lock SH (read) agian...\n"; }

		}
		else { print "Unable to lock SH (read)...\n"; }
	}
}
# END BLOCK for debugging only, will be removed at compact release | NL_CODE: /RM_BLOCK
1;
__END__

=head1 NAME

NL::File::Lock - Extended file locking (based on lockfiles)

=head1 SYNOPSIS

	use NL::File::Lock;

	# We will create locks for that file:
	my $filename = 'test_file.txt';

	# Writing all lockfiles to '/tmp' directory:
	&NL::File::Lock::init('/tmp');
	# If no 'NL::File::Lock::init' called - all lock files
	# will be at the same directorys as files

	# ---
	# Lock for writing (only one process can write)
	# 'timeout' - time to wail lock
	# 'time_sleep' - time to sleep between locking retrys
	if (&NL::File::Lock::lock_write($filename, { 'timeout' => 10, 'time_sleep' => 0.1 } )) {
		# File locked
		# ... code ...
		&NL::File::Lock::unlock($filename);
	}
	else {
		# Unable to lock file
	}

	# ---
	# Lock for reading (many processes can read)
	# 'timeout' - time to wail lock
	# 'time_sleep' - time to sleep between locking retrys
	if (&NL::File::Lock::lock_read($filename, { 'timeout' => 10, 'time_sleep' => 0.1 } )) {
		# File locked
		# ... code ...
		&NL::File::Lock::unlock($filename);
	}
	else {
		# Unable to lock file
	}

=head1 DESCRIPTION

This module is used to powerful, easy and portable file locking.

=head1 EXAMPLES

	# ---
	# Lock for writing (only one process can write)
	my $filename = 'test_file.txt';
	# 'timeout' - time to wail lock
	# 'time_sleep' - time to sleep between locking retrys
	if (&NL::File::Lock::lock_write($filename, { 'timeout' => 10, 'time_sleep' => 0.1 } )) {
		print "+Locked EX (write)...\n";
		sleep(5);
		&NL::File::Lock::unlock_not_unlink($filename);
		print "-UnLocked for some time...\n";
		sleep(5);
		if (&NL::File::Lock::lock_write($filename, { 'timeout' => 10, 'time_sleep' => 0.1 } )) {
			print "+Locked EX (write)...\n";
			sleep(5);
			&NL::File::Lock::unlock($filename);
			print "-UnLocked forever...\n";
			sleep(5);
		}
		else { print "Unable to lock EX (write) again...\n"; }
	}
	else { print "Unable to lock EX (write)...\n"; }

	# ---
	# Lock for reading (many processes can read)
	my $filename = 'test_file.txt';
	# 'timeout' - time to wail lock
	# 'time_sleep' - time to sleep between locking retrys
	if (&NL::File::Lock::lock_read($filename, { 'timeout' => 10, 'time_sleep' => 0.1 } )) {
			print "+Locked SH (read)...\n";
			sleep(5);
			&NL::File::Lock::unlock_not_unlink($filename);
			print "-UnLocked for some time...\n";
			sleep(5);
			if (&NL::File::Lock::lock_read($filename, { 'timeout' => 10, 'time_sleep' => 0.1 } )) {
				print "+Locked SH (read)...\n";
				sleep(5);
				&NL::File::Lock::unlock($filename);
				print "-UnLocked forever...\n";
				sleep(5);
			}
			else { print "Unable to lock SH (read) agian...\n"; }
	}
	else { print "Unable to lock SH (read)...\n"; }

=head1 AUTHOR

 Nickolay Kovalev, http://resume.nickola.ru
 Email: nickola_code@nickola.ru

=cut
