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

    $self->_request('PUT', $self->_build_uri([$bucket]), '204',
        encode_json{schema => $schema});
}

sub list_bucket {
    my ( $self, $bucket ) = @_;
    return $self->_request('GET', $self->_build_uri([$bucket]), '200');
}

sub fetch {
    my ($self, $bucket, $key) = @_;
    return $self->_request('GET', $self->_build_uri([$bucket, $key]), '200');
}

sub store {
    my ( $self, $object ) = @_;

    my $bucket = $object->{bucket};
    my $key    = $object->{key};
    return $self->_request(
        'PUT',
        $self->_build_uri(
            [ $bucket, $key ],
            {
                dw         => 2,
                returnbody => 'true'
            }
        ),
        '200',
        encode_json $object);
}

sub fetch {
    my ( $self, $bucket, $key, ) = @_;

    return $self->_request( 'GET',
        $self->_build_uri( [ $bucket, $key ], { r => 2 } ), '200' );
}

sub delete {
    my ( $self, $bucket, $key ) = @_;

    return $self->_request( 'DELETE',
        $self->_build_uri( [ $bucket, $key ], { dw => 2 } ), 204 );
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
            return $cv->send(
                $headers->{Status} . ' : ' . $headers->{Reason} );
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

AnyEvent::Riak -

=head1 SYNOPSIS

  use AnyEvent::Riak;

=head1 DESCRIPTION

AnyEvent::Riak is

=head1 AUTHOR

franck cuny E<lt>franck.cuny {at} rtgi.frE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
