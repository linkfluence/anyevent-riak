use strict;
use warnings;
use Test::More;
use Test::Exception;
use AnyEvent::Riak;

my $jiak = AnyEvent::Riak->new(
    host => 'http://127.0.0.1:8098',
    path => 'jiak'
);

ok my $buckets = $jiak->list_bucket('bar')->recv, "... fetch bucket list";
is scalar @{ $buckets->{keys} }, '0', '... no keys';

ok my $new_bucket
    = $jiak->set_bucket( 'foo', { allowed_fields => '*' } )->recv,
    '... set a new bucket';

my $value = {
    bucket => 'foo',
    key    => 'baz',
    object => { foo => "bar" },
    links  => []
};

ok my $res = $jiak->store($value)->recv, '... set a new key';

ok $res = $jiak->fetch( 'foo', 'baz' )->recv, '... fetch our new key';
ok $res = $jiak->delete( 'foo', 'baz' )->recv, '... delete our key';

done_testing();
