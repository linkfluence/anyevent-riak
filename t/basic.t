use strict;
use warnings;
use Test::More;
use JSON::XS;
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
    key    => 'bar',
    object => { foo => "bar", baz => 1 },
    links  => []
};

ok my $res = $jiak->store($value)->recv, '... set a new key';

ok $res = $jiak->fetch( 'foo', 'bar' )->recv, '... fetch our new key';
ok $res = $jiak->delete( 'foo', 'bar' )->recv, '... delete our key';

dies_ok { $jiak->fetch( 'foo', 'foo' )->recv } '... dies when error';
like $@, qr/404/, '... 404 response';

done_testing();
