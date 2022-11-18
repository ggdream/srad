package srad

import (
	"context"
	"testing"
	"time"
)

func TestRegister(t *testing.T) {
	r, err := Register(context.Background(), "mm", "auth", "127.0.0.1", 10001, 2, []string{"127.0.0.1:2379"})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Minute)

	err = r.Unregister()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDiscover(t *testing.T) {
	cc, err := Discover("mm", "auth", []string{"127.0.0.1:2379"})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%#v\n", cc)
}
