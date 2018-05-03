package gowfs

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
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
	Token string

	nns    []string
	curnn  string
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
		if cl.opts.autoGenToken {
			if cl.Token, err = cl.GetDelegationToken(); err != nil {
				return nil, err
			}
			go cl.renewToken()
		}
	}
	return cl, err
}

func (cl *Client) renewToken() {
	var (
		retry    int
		backoff  = time.Second * 10
		fixAhead = time.Minute * 30
		timer    *time.Timer
	)
	for {
		d, err := cl.getTokenExpiration()
		if err != nil {
			if retry > 3 {
				if cl.Token, err = cl.GetDelegationToken(); err != nil {
					return
				}
				retry = 0
				continue
			}
			time.Sleep(backoff * time.Duration(retry))
			retry++
			continue
		}
		r := time.Duration(rand.Int63n(int64(time.Minute * 10)))
		if d-fixAhead-r > 0 {
			d = d - fixAhead - r
		}
		if timer == nil {
			timer = time.NewTimer(d)
		} else {
			timer.Reset(d)
		}
		<-timer.C
	}
}

func (cl *Client) getTokenExpiration() (d time.Duration, err error) {
	var exp int64
	exp, err = cl.RenewDelegationToken()
	if err != nil {
		return 0, err
	}
	d = time.Unix(exp/1000, 0).Sub(time.Now())
	if d <= 0 {
		d = time.Hour
	}
	return d, nil
}

func (cl *Client) resolveNameNodes() {
	cl.curnn = ""
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
