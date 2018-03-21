package gowfs

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"time"

	"github.com/json-iterator/go"
)

const (
	JMX_URL = "http://%s/jmx?get=Hadoop:service=NameNode,name=FSNamesystem::tag.HAState"
)

var (
	errInvalidNameNode     = errors.New("invalid name node")
	errNoAvailableNameNode = errors.New("no available name node")
	errInvalidOperation    = errors.New("invalid operation")
	errRemoteException     = errors.New("recive a remote excpetion")
	errInvalidResponse     = errors.New("invalid format of response")
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type jmxResp struct {
	Beans []struct {
		HAState string `json:"tag.HAState"`
	} `json:"beans"`
}

func defaultTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,
	}
}

type Client struct {
	nns    []string
	curnn  string
	token  string
	httpcl *http.Client
	opts   clientOptions
}

// NewClient returns a client wrapping webhdfs APIs.
// If your HDFS cluster support HA, please at least set two name nodes,
// and the Client use jmx metrics to get the available one.
func NewClient(nameNodes []string, opts ...ClientOption) (*Client, error) {
	var err error
	if len(nameNodes) == 0 {
		return nil, errInvalidNameNode
	}
	cl := &Client{
		nns: nameNodes,
	}
	for _, opt := range opts {
		opt(&cl.opts)
	}
	if cl.opts.transport == nil {
		cl.opts.transport = defaultTransport()
	}
	cl.httpcl = &http.Client{
		Transport: cl.opts.transport,
	}

	cl.curnn = cl.nns[0]
	if len(cl.nns) > 1 {
		cl.resolveNameNodes()
		if cl.curnn == "" {
			return nil, errNoAvailableNameNode
		}
	}

	// kerberos authorization
	if cl.opts.krb5cl != nil {
		err = cl.opts.krb5cl.Login()
		if err != nil {
			return nil, err
		}
		if token, err := cl.GetDelegationToken(); err == nil {
			cl.token = token
		}
	}
	return cl, err
}

func (cl *Client) resolveNameNodes() {
	for _, nn := range cl.nns {
		url := fmt.Sprintf(JMX_URL, nn)
		resp, err := cl.httpcl.Get(url)
		if err != nil {
			continue
		}
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		var r jmxResp
		err = json.Unmarshal(b, &r)
		if err != nil {
			continue
		}
		if len(r.Beans) > 0 && r.Beans[0].HAState == "active" {
			cl.curnn = nn
			break
		}
	}
}
