package zk_wrapper

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/meitu/go-zookeeper/zk"
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

func (c *Conn) AppendChroot(path string) string {
	if c.chroot != "" {
		return c.chroot + path
	}
	return path
}

func (c *Conn) TrimChroot(path string) string {
	if c.chroot != "" {
		return strings.TrimPrefix(path, c.chroot)
	}
	return path
}

func (c *Conn) Children(path string) ([]string, *zk.Stat, error) {
	children, stat, err := c.Conn.Children(c.AppendChroot(path))
	res := make([]string, len(children))
	for i, child := range children {
		res[i] = c.TrimChroot(child)
	}
	return res, stat, err
}

func (c *Conn) ChildrenW(path string) ([]string, *zk.Stat, *zk.Watcher, error) {
	children, stat, w, err := c.Conn.ChildrenW(c.AppendChroot(path))
	res := make([]string, len(children))
	for i, child := range children {
		res[i] = c.TrimChroot(child)
	}
	return res, stat, w, err
}

func (c *Conn) Get(path string) ([]byte, *zk.Stat, error) {
	return c.Conn.Get(c.AppendChroot(path))
}

func (c *Conn) GetW(path string) ([]byte, *zk.Stat, *zk.Watcher, error) {
	return c.Conn.GetW(c.AppendChroot(path))
}

func (c *Conn) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	return c.Conn.Set(c.AppendChroot(path), data, version)
}

func (c *Conn) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	zkPath, err := c.Conn.Create(c.AppendChroot(path), data, flags, acl)
	return c.TrimChroot(zkPath), err
}

func (c *Conn) CreateProtectedEphemeralSequential(path string, data []byte, acl []zk.ACL) (string, error) {
	zkPath, err := c.Conn.CreateProtectedEphemeralSequential(c.AppendChroot(path), data, acl)
	return c.TrimChroot(zkPath), err
}

func (c *Conn) Delete(path string, version int32) error {
	return c.Conn.Delete(c.AppendChroot(path), version)
}

func (c *Conn) Exists(path string) (bool, *zk.Stat, error) {
	return c.Conn.Exists(c.AppendChroot(path))
}

func (c *Conn) ExistsW(path string) (bool, *zk.Stat, *zk.Watcher, error) {
	return c.Conn.ExistsW(c.AppendChroot(path))
}

func (c *Conn) GetACL(path string) ([]zk.ACL, *zk.Stat, error) {
	return c.Conn.GetACL(c.AppendChroot(path))
}

func (c *Conn) SetACL(path string, acl []zk.ACL, version int32) (*zk.Stat, error) {
	return c.Conn.SetACL(c.AppendChroot(path), acl, version)
}

func (c *Conn) Sync(path string) (string, error) {
	zkPath, err := c.Conn.Sync(c.AppendChroot(path))
	return c.TrimChroot(zkPath), err
}

func (c *Conn) Multi(ops ...interface{}) ([]zk.MultiResponse, error) {
	for i, op := range ops {
		switch op.(type) {
		case *zk.CreateRequest:
			ops[i].(*zk.CreateRequest).Path = c.AppendChroot(op.(*zk.CreateRequest).Path)
		case *zk.SetDataRequest:
			ops[i].(*zk.SetDataRequest).Path = c.AppendChroot(op.(*zk.SetDataRequest).Path)
		case *zk.DeleteRequest:
			ops[i].(*zk.DeleteRequest).Path = c.AppendChroot(op.(*zk.DeleteRequest).Path)
		case *zk.CheckVersionRequest:
			ops[i].(*zk.CheckVersionRequest).Path = c.AppendChroot(op.(*zk.CheckVersionRequest).Path)
		}
	}
	res, err := c.Multi(ops...)
	for i, op := range res {
		res[i].String = c.TrimChroot(op.String)
	}
	return res, err
}

func (c *Conn) mkEmptyDirRecursive(zkPath string) error {
	if zkPath == "/" {
		return nil
	}
	parent := path.Dir(zkPath)
	if parent != "/" {
		if err := c.mkEmptyDirRecursive(parent); err != nil {
			return err
		}
	}
	_, err := c.Create(zkPath, nil, 0, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		return nil
	}
	return err
}

func (c *Conn) MkNodeRecursive(zkPath string, flags int32, data []byte) error {
	_, err := c.Set(zkPath, data, -1)
	if err == nil {
		return nil
	}
	if err != zk.ErrNoNode {
		return err
	}
	if zkPath == "/" {
		return errors.New("root path can't deleted")
	}
	// create the parent dir first if not exists
	if err := c.mkEmptyDirRecursive(path.Dir(zkPath)); err != nil {
		return err
	}
	_, err = c.Create(zkPath, data, flags, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists { // overwrite the data if exists
		_, err = c.Set(zkPath, data, -1)
	}
	return err
}

func (c *Conn) DeleteRecursive(zkPath string, version int32) error {
	// return err if nil or not the not node empty error
	if err := c.Delete(zkPath, version); err == nil || err != zk.ErrNotEmpty {
		return err
	}
	// remove the create/write perm when we are deleting the path
	perm := int32(zk.PermAdmin | zk.PermDelete | zk.PermRead)
	if _, err := c.SetACL(zkPath, zk.WorldACL(perm), version); err != nil {
		return err
	}
	childs, _, err := c.Children(zkPath)
	if err != nil {
		return err
	}
	for _, child := range childs {
		// Don't use path.Join while conflict with param name
		err = c.DeleteRecursive(path.Join(zkPath, child), -1)
		if err != nil && err != zk.ErrNoNode {
			return err
		}
	}
	return c.Delete(zkPath, version)
}

func (c *Conn) RemoveWatcher(w *zk.Watcher) bool {
	return c.Conn.RemoveWatcher(w)
}
