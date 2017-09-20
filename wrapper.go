package zk_wrapper

import (
	"fmt"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type Conn struct {
	*zk.Conn
	chroot string
}

func Connect(servers []string, sessionTimeout time.Duration) (*Conn, <-chan zk.Event, error) {
	zkConn, ev, err := zk.Connect(servers, sessionTimeout)
	return &Conn{zkConn, ""}, ev, err
}

func ConnectWithDialer(servers []string, sessionTimeout time.Duration, dialer zk.Dialer) (*Conn, <-chan zk.Event, error) {
	zkConn, ev, err := zk.Connect(servers, sessionTimeout, zk.WithDialer(dialer))
	return &Conn{zkConn, ""}, ev, err
}

func (c *Conn) Chroot(prefix string) error {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" { // undo chroot
		c.chroot = ""
		return nil
	}
	exists, _, err := c.Conn.Exists(prefix)
	if err != nil {
		return fmt.Errorf("failed to chroot, err: %s", err)
	}
	if !exists {
		return fmt.Errorf("failed to chroot, err: chroot %s doesn't exist", prefix)
	}
	c.chroot = prefix
	return nil
}

func (c *Conn) appendPrefix(path string) string {
	if c.chroot != "" {
		return c.chroot + path
	}
	return path
}

func (c *Conn) trimPrefix(path string) string {
	if c.chroot != "" {
		return strings.TrimPrefix(path, c.chroot)
	}
	return path
}

func (c *Conn) Children(path string) ([]string, *zk.Stat, error) {
	children, stat, err := c.Conn.Children(c.appendPrefix(path))
	res := make([]string, len(children))
	for i, child := range children {
		res[i] = c.trimPrefix(child)
	}
	return res, stat, err
}

func (c *Conn) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	children, stat, ev, err := c.Conn.ChildrenW(c.appendPrefix(path))
	res := make([]string, len(children))
	for i, child := range children {
		res[i] = c.trimPrefix(child)
	}
	return res, stat, ev, err
}

func (c *Conn) Get(path string) ([]byte, *zk.Stat, error) {
	return c.Conn.Get(c.appendPrefix(path))
}

func (c *Conn) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	return c.Conn.GetW(c.appendPrefix(path))
}

func (c *Conn) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	return c.Conn.Set(c.appendPrefix(path), data, version)
}

func (c *Conn) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	zkPath, err := c.Conn.Create(c.appendPrefix(path), data, flags, acl)
	return c.trimPrefix(zkPath), err
}

func (c *Conn) CreateProtectedEphemeralSequential(path string, data []byte, acl []zk.ACL) (string, error) {
	zkPath, err := c.Conn.CreateProtectedEphemeralSequential(c.appendPrefix(path), data, acl)
	return c.trimPrefix(zkPath), err
}

func (c *Conn) Delete(path string, version int32) error {
	return c.Conn.Delete(c.appendPrefix(path), version)
}

func (c *Conn) Exists(path string) (bool, *zk.Stat, error) {
	return c.Conn.Exists(c.appendPrefix(path))
}

func (c *Conn) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	return c.Conn.ExistsW(c.appendPrefix(path))
}

func (c *Conn) GetACL(path string) ([]zk.ACL, *zk.Stat, error) {
	return c.Conn.GetACL(c.appendPrefix(path))
}

func (c *Conn) SetACL(path string, acl []zk.ACL, version int32) (*zk.Stat, error) {
	return c.Conn.SetACL(c.appendPrefix(path), acl, version)
}

func (c *Conn) Sync(path string) (string, error) {
	zkPath, err := c.Conn.Sync(c.appendPrefix(path))
	return c.trimPrefix(zkPath), err
}

func (c *Conn) Multi(ops ...interface{}) ([]zk.MultiResponse, error) {
	for i, op := range ops {
		switch op.(type) {
		case *zk.CreateRequest:
			ops[i].(*zk.CreateRequest).Path = c.appendPrefix(op.(*zk.CreateRequest).Path)
		case *zk.SetDataRequest:
			ops[i].(*zk.SetDataRequest).Path = c.appendPrefix(op.(*zk.SetDataRequest).Path)
		case *zk.DeleteRequest:
			ops[i].(*zk.DeleteRequest).Path = c.appendPrefix(op.(*zk.DeleteRequest).Path)
		case *zk.CheckVersionRequest:
			ops[i].(*zk.CheckVersionRequest).Path = c.appendPrefix(op.(*zk.CheckVersionRequest).Path)
		}
	}
	res, err := c.Multi(ops...)
	for i, op := range res {
		res[i].String = c.trimPrefix(op.String)
	}
	return res, err
}
