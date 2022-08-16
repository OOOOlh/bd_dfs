package client

type Client struct {
	// namenode *rpc.NamenodeConnection
	Option ClientOptions
}

type ClientOptions struct {
	// Addresses specifies the namenode(s) to connect to.
	Addresses []string
}

func NewClient() (*Client, error) {
	// client := Client{}
	return &Client{}, nil
}