package zk_wrapper

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	root = "/chroot"
	conn *Conn
)

func init() {
	var err error
	conn, _, err = Connect([]string{"127.0.0.1:2181"}, 6*time.Second)
	if err != nil {
		fmt.Printf("Failed to setup test, err: %s", err)
		os.Exit(1)
	}
	_, err = conn.Create(root, nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		fmt.Printf("Failed to create root path, err: %s", err)
		os.Exit(1)
	}
}

func TestAppendPrefix(t *testing.T) {
	cases := []string{
		"/a",
		"/b/c",
	}
	mockConn := &Conn{
		chroot: root,
	}
	for _, c := range cases {
		expected := root + c
		got := mockConn.appendPrefix(c)
		if got != expected {
			t.Errorf("%s was expected, but got %s", expected, got)
		}
	}
}

func TestTrimPrefix(t *testing.T) {
	cases := []string{
		"/a",
		"/b/c",
	}
	mockConn := &Conn{
		chroot: root,
	}
	for _, c := range cases {
		input := mockConn.appendPrefix(c)
		got := mockConn.trimPrefix(input)
		if c != mockConn.trimPrefix(input) {
			t.Errorf("%s was expected, but got %s", c, got)
		}
	}
}

func TestCreate(t *testing.T) {
	path := "/create-test"
	conn.Chroot(root)
	zkPath, err := conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
	if zkPath != path && err != zk.ErrNodeExists {
		t.Errorf("%s was expected, but got %s", path, zkPath)
	}
	conn.Chroot("")
	exists, _, err := conn.Exists(root + path)
	if !exists || err != nil {
		t.Errorf("exists was expected, but got not exists")
	}
}

func TestGetAndSet(t *testing.T) {
	path := "/get-set-test"
	data := []byte{1, 2, 3, 4}
	conn.Chroot(root)
	conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
	_, err := conn.Set(path, data, -1)
	if err != nil {
		t.Errorf("success to Set was expected, but got err %s", err)
	}
	v, _, err := conn.Get(path)
	if err != nil {
		t.Errorf("success to Get was expected, but got err %s", err)
	}
	if v == nil {
		t.Errorf("data %v was expected, but got nil", data)
	}
	conn.Chroot("")
	v1, _, err := conn.Get(root + path)
	if err != nil {
		t.Errorf("success to Get was expected, but got err %s", err)
	}
	if v1 == nil {
		t.Errorf("%v was expected, but got nil", data)
	}
}

func TestDelete(t *testing.T) {
	path := "/delete-test"
	conn.Create(root+path, nil, 0, zk.WorldACL(zk.PermAll))
	conn.Chroot(root)
	err := conn.Delete(path, -1)
	if err != nil {
		t.Errorf("ret nil was expected, but got err %s", err)
	}
}

func TestChildren(t *testing.T) {
	path := "/children-test"
	conn.Chroot("")
	_, err := conn.Create(root+path, nil, 0, zk.WorldACL(zk.PermAll))
	fmt.Println(root+path, err)
	subPaths := []string{"a", "b", "c"}
	for _, sub := range subPaths {
		_, err = conn.Create(root+path+"/"+sub, nil, 0, zk.WorldACL(zk.PermAll))
	}
	conn.Chroot(root)
	children, _, _ := conn.Children(path)
	if len(children) != len(subPaths) {
		t.Errorf("children num %d was expected, but got %d", len(subPaths), len(children))
	}
}
