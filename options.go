package gowfs

import (
	"net/http"

	gokrb5cl "gopkg.in/jcmturner/gokrb5.v4/client"
)

type ClientOption func(*clientOptions)

type clientOptions struct {
	user      string
	krb5cl    *gokrb5cl.Client
	transport *http.Transport
}

func WithKerberos(cl *gokrb5cl.Client) ClientOption {
	return func(o *clientOptions) {
		o.krb5cl = cl
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
