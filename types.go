package gowfs

import "fmt"

type (
	OperationParams struct {
		Addr       string
		Op         string
		Path       string
		UserName   string
		Delegation string
		Offset     uint64
		Length     uint64
		BufferSize uint32
	}

	Response struct {
		Boolean         bool             `json:"boolean"`
		FileStatus      *FileStatus      `json:",omitempty"`
		FileStatuses    *FileStatuses    `json:",omitempty"`
		FileChecksum    *FileChecksum    `json:",omitempty"`
		ContentSummary  *ContentSummary  `json:",omitempty"`
		Token           *Token           `json:",omitempty"`
		Tokens          *Tokens          `json:",omitempty"`
		RemoteException *RemoteException `json:",omitempty"`
	}

	FileStatus struct {
		AccesTime        int64
		BlockSize        int64
		Group            string
		Length           int64
		ModificationTime int64
		Owner            string
		PathSuffix       string
		Permission       string
		Replication      int64
		Type             string
	}

	FileStatuses struct {
		FileStatus []FileStatus
	}
	FileChecksum struct {
		Algorithm string
		Bytes     string
		Length    int64
	}

	ContentSummary struct {
		DirectoryCount int64
		FileCount      int64
		Length         int64
		Quota          int64
		SpaceConsumed  int64
		SpaceQuota     int64
	}
	Token struct {
		UrlString string
	}

	Tokens struct {
		Token []Token
	}

	RemoteException struct {
		Exception     string
		JavaClassName string
		Message       string
	}
)

// Implementation of error type.  Returns string representation of RemoteException
func (re *RemoteException) Error() string {
	return fmt.Sprintf("RemoteException: %v [%v]\n[%v]\n", re.Exception, re.JavaClassName, re.Message)
}