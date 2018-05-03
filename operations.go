package gowfs

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"text/template"
)

// Operations of WebHDFS
const (
	OP_OPEN                  = "OPEN"
	OP_CREATE                = "CREATE"
	OP_APPEND                = "APPEND"
	OP_CONCAT                = "CONCAT"
	OP_RENAME                = "RENAME"
	OP_DELETE                = "DELETE"
	OP_SETPERMISSION         = "SETPERMISSION"
	OP_SETOWNER              = "SETOWNER"
	OP_SETREPLICATION        = "SETREPLICATION"
	OP_SETTIMES              = "SETTIMES"
	OP_MKDIRS                = "MKDIRS"
	OP_CREATESYMLINK         = "CREATESYMLINK"
	OP_LISTSTATUS            = "LISTSTATUS"
	OP_GETFILESTATUS         = "GETFILESTATUS"
	OP_GETCONTENTSUMMARY     = "GETCONTENTSUMMARY"
	OP_GETFILECHECKSUM       = "GETFILECHECKSUM"
	OP_GETDELEGATIONTOKEN    = "GETDELEGATIONTOKEN"
	OP_GETDELEGATIONTOKENS   = "GETDELEGATIONTOKENS"
	OP_RENEWDELEGATIONTOKEN  = "RENEWDELEGATIONTOKEN"
	OP_CANCELDELEGATIONTOKEN = "CANCELDELEGATIONTOKEN"
)

var (
	opURLs map[string]*template.Template
)

func init() {
	opURLs = make(map[string]*template.Template)
	opURLs[OP_OPEN] = template.Must(template.New("").Parse("http://{{.Addr}}/webhdfs/v1/{{.Path}}/?op={{.Op}}{{if .Offset}}&offset={{.Offset}}{{end}}{{if .Length}}&length={{.Length}}{{end}}{{if .BufferSize}}&buffersize={{.BufferSize}}{{end}}{{if .Delegation}}&delegation={{.Delegation}}{{end}}{{if .UserName}}&user.name={{.UserName}}{{end}}"))
	opURLs[OP_GETFILESTATUS] = template.Must(template.New("").Parse("http://{{.Addr}}/webhdfs/v1/{{.Path}}/?op={{.Op}}{{if .Delegation}}&delegation={{.Delegation}}{{end}}{{if .UserName}}&user.name={{.UserName}}{{end}}"))
	opURLs[OP_LISTSTATUS] = template.Must(template.New("").Parse("http://{{.Addr}}/webhdfs/v1/{{.Path}}/?op={{.Op}}{{if .Delegation}}&delegation={{.Delegation}}{{end}}{{if .UserName}}&user.name={{.UserName}}{{end}}"))
	opURLs[OP_GETFILECHECKSUM] = template.Must(template.New("").Parse("http://{{.Addr}}/webhdfs/v1/{{.Path}}/?op={{.Op}}{{if .Delegation}}&delegation={{.Delegation}}{{end}}{{if .UserName}}&user.name={{.UserName}}{{end}}"))
	opURLs[OP_GETDELEGATIONTOKEN] = template.Must(template.New("").Parse("http://{{.Addr}}/webhdfs/v1/?op={{.Op}}{{if .UserName}}&renewer={{.UserName}}{{end}}"))
	opURLs[OP_RENEWDELEGATIONTOKEN] = template.Must(template.New("").Parse("http://{{.Addr}}/webhdfs/v1/?op={{.Op}}&token={{.Delegation}}"))
}

func (cl *Client) getOperationURL(params *OperationParams) (string, error) {
	tpl, ok := opURLs[params.Op]
	if !ok {
		return "", errInvalidOperation
	}
	if cl.opts.krb5cl != nil {
		params.Delegation = cl.Token
	} else if cl.opts.user != "" {
		params.UserName = cl.opts.user
	}
	if len(cl.nns) > 1 {
		cl.resolveNameNodes()
	}
	params.Addr = cl.curnn

	var b strings.Builder
	err := tpl.Execute(&b, params)
	return b.String(), err
}

func (cl *Client) doJSONRequest(req *http.Request) (Response, error) {
	resp, err := cl.httpcl.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	return handleJSONResponse(resp.Body)
}

func (cl *Client) GetDelegationToken() (token string, err error) {
	url, err := cl.getOperationURL(&OperationParams{
		Op: OP_GETDELEGATIONTOKEN,
	})
	if err != nil {
		return
	}
	req, _ := http.NewRequest("GET", url, nil)
	if cl.opts.krb5cl != nil {
		cl.opts.krb5cl.SetSPNEGOHeader(req, "")
	}
	resp, err := cl.doJSONRequest(req)
	if err != nil {
		return
	}
	if resp.Token == nil {
		err = errInvalidResponse
		return
	}
	return resp.Token.UrlString, err
}

func (cl *Client) RenewDelegationToken() (expiration int64, err error) {
	url, err := cl.getOperationURL(&OperationParams{
		Op: OP_RENEWDELEGATIONTOKEN,
	})
	if err != nil {
		return
	}
	req, _ := http.NewRequest("PUT", url, nil)
	if cl.opts.krb5cl != nil {
		cl.opts.krb5cl.SetSPNEGOHeader(req, "")
	}
	resp, err := cl.doJSONRequest(req)
	if err != nil {
		return
	}
	if resp.Long == 0 {
		err = errInvalidResponse
		return
	}
	return resp.Long, err
}

func (cl *Client) Open(path string, offset, length uint64, bufferSize uint32) (io.ReadCloser, error) {
	params := &OperationParams{
		Op:         OP_OPEN,
		Path:       path,
		Offset:     offset,
		Length:     length,
		BufferSize: bufferSize,
	}
	url, err := cl.getOperationURL(params)
	if err != nil {
		return nil, err
	}
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := cl.httpcl.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		_, err := handleJSONResponse(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("open path(%s) failed for server return code '%d'", path, resp.StatusCode)
	}
	return resp.Body, nil
}

func (cl *Client) ListStatus(path string) ([]FileStatus, error) {
	params := &OperationParams{
		Op:   OP_LISTSTATUS,
		Path: path,
	}
	url, err := cl.getOperationURL(params)
	if err != nil {
		return nil, err
	}
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := cl.httpcl.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	r, err := handleJSONResponse(resp.Body)
	if err != nil {
		return nil, err
	}
	if r.FileStatuses == nil {
		return nil, errInvalidResponse
	}
	return r.FileStatuses.FileStatus, nil
}

func handleJSONResponse(r io.Reader) (resp Response, err error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &resp)
	if resp.RemoteException != nil {
		err = resp.RemoteException
		return
	}
	return
}
