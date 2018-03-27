package gowfs

import (
	"net/http"

	gokrb5cl "gopkg.in/jcmturner/gokrb5.v4/client"
)

type ClientOption func(*clientOptions)

type clientOptions struct {
	// hdfs user name
	user string

	// kerberos client
	krb5cl *gokrb5cl.Client

	// auto generate and renew delegation token
	autoGenToken bool

	// http transport
	transport *http.Transport
}

func WithKerberos(cl *gokrb5cl.Client, autoGenToken bool) ClientOption {
	return func(o *clientOptions) {
		o.krb5cl = cl
		o.autoGenToken = autoGenToken
	}
}

func WithTransport(t *http.Transport) ClientOption {
	return func(o *clientOptions) {
		o.transport = t
	}
}

func WithUser(user string) ClientOption {
	return func(o *clientOptions) {
		o.user = user
	}
}
