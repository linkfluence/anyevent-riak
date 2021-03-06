package AnyEvent::Riak;

use strict;
use Carp;
use URI;
use JSON::XS;
use AnyEvent;
use AnyEvent::HTTP;

our $VERSION = '0.01';

sub new {
    my ( $class, %args ) = @_;

    my $host = delete $args{host} || 'http://127.0.0.1:8098';
    my $path = delete $args{path} || 'jiak';

    bless {
        host => $host,
        path => $path,
        %args,
    }, $class;
}

sub set_bucket {
    my ( $self, $bucket, $schema ) = @_;

    carp "your schema is missing allowed_fields"
        if ( !exists $schema->{allowed_fields} );

    if ( !exists $schema->{required_fields} ) {
        $schema->{required_fields} = [];
    }
    if ( !exists $schema->{read_mask} ) {
        $schema->{read_mask} = $schema->{allowed_fields};
    }
    if ( !exists $schema->{write_mask} ) {
        $schema->{write_mask} = $schema->{read_mask};
    }

    $self->_request(
        'PUT', $self->_build_uri( [$bucket] ),
        '204', encode_json { schema => $schema }
    );
}

sub list_bucket {
    my ( $self, $bucket ) = @_;
    return $self->_request( 'GET', $self->_build_uri( [$bucket] ), '200' );
}

sub fetch {
    my ( $self, $bucket, $key, $r ) = @_;
    $r = $self->{r} || 2 if !$r;
    return $self->_request( 'GET',
        $self->_build_uri( [ $bucket, $key ], { r =>  $r} ), '200' );
}

sub store {
    my ( $self, $object, $w, $dw, ) = @_;

    $w  = $self->{w}  || 2 if !$w;
    $dw = $self->{dw} || 2 if !$dw;

    my $bucket = $object->{bucket};
    my $key    = $object->{key};
    $object->{links} = [] if !exists $object->{links};

    return $self->_request(
        'PUT',
        $self->_build_uri(
            [ $bucket, $key ],
            {
                w          => $w,
                dw         => $dw,
                returnbody => 'true'
            }
        ),
        '200',
        encode_json $object);
}

sub delete {
    my ( $self, $bucket, $key, $rw ) = @_;

    $rw = $self->{rw} || 2 if !$rw;
    return $self->_request( 'DELETE',
        $self->_build_uri( [ $bucket, $key ], { dw => $rw } ), 204 );
}

sub _build_uri {
    my ( $self, $path, $query ) = @_;
    my $uri = URI->new( $self->{host} );
    $uri->path( $self->{path} . "/" . join( "/", @$path ) );
    $uri->query_form(%$query) if $query;
    return $uri->as_string;
}

sub _request {
    my ( $self, $method, $uri, $expected, $body ) = @_;
    my $cv = AnyEvent->condvar;
    my $cb = sub {
        my ( $body, $headers ) = @_;
        if ( $headers->{Status} == $expected ) {
            $body
                ? return $cv->send( decode_json($body) )
                : return $cv->send(1);
        }
        else {
            return $cv->croak(
                encode_json( [ $headers->{Status}, $headers->{Reason} ] ) );
        }
    };
    if ($body) {
        http_request(
            $method => $uri,
            headers => { 'Content-Type' => 'application/json', },
            body    => $body,
            $cb
        );
    }
    else {
        http_request(
            $method => $uri,
            headers => { 'Content-Type' => 'application/json', },
            $cb
        );
    }
    $cv;
}

1;
__END__

=head1 NAME

AnyEvent::Riak - Non-blocking Riak client

=head1 SYNOPSIS

  use AnyEvent::Riak;

  my $riak = AnyEvent::Riak->new(
    host => 'http://127.0.0.1:8098',
    path => 'jiak',
  );

  my $buckets    = $riak->list_bucket('namespace')->recv;
  my $new_bucket = $riak->set_bucket('foo', {allowed_fields => '*'})->recv;
  my $store      = $riak->store({bucket => 'foo', key => 'bar', object => {baz => 1},link => []})->recv;
  my $fetch      = $riak->fetch('foo', 'bar')->recv;
  my $delete     = $riak->delete('foo', 'bar')->recv;

=head1 DESCRIPTION

AnyEvent::Riak is a non-blocking riak client using anyevent.

=head1 AUTHOR

franck cuny E<lt>franck@lumberjaph.netE<gt>

=head1 SEE ALSO

=head1 LICENSE

Copyright 2009 by linkfluence.

L<http://linkfluence.net>

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
