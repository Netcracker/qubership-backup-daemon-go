package utils

import (
	"testing"

	gomock "go.uber.org/mock/gomock"
)

func TestS3AliasRegistry_Get_Known(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := NewMockS3ClientRepository(ctrl)
	reg := NewS3AliasRegistry(map[string]S3ClientRepository{
		"prod": client,
	})

	got, err := reg.Get("prod")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != client {
		t.Error("expected the same client instance")
	}
}

func TestS3AliasRegistry_Get_Unknown(t *testing.T) {
	reg := NewS3AliasRegistry(map[string]S3ClientRepository{})

	_, err := reg.Get("missing")
	if err == nil {
		t.Fatal("expected error for unknown alias")
	}
}

func TestS3AliasRegistry_Has(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := NewMockS3ClientRepository(ctrl)
	reg := NewS3AliasRegistry(map[string]S3ClientRepository{
		"default": client,
	})

	if !reg.Has("default") {
		t.Error("expected Has to return true for existing alias")
	}
	if reg.Has("nonexistent") {
		t.Error("expected Has to return false for missing alias")
	}
}

func TestS3AliasRegistry_Nil_Map(t *testing.T) {
	reg := NewS3AliasRegistry(nil)

	if reg.Has("anything") {
		t.Error("expected Has to return false for nil map")
	}
	_, err := reg.Get("anything")
	if err == nil {
		t.Fatal("expected error for nil map")
	}
}

func TestS3AliasRegistry_Multiple_Aliases(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	clientA := NewMockS3ClientRepository(ctrl)
	clientB := NewMockS3ClientRepository(ctrl)

	reg := NewS3AliasRegistry(map[string]S3ClientRepository{
		"alias-a": clientA,
		"alias-b": clientB,
	})

	gotA, err := reg.Get("alias-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotA != clientA {
		t.Error("expected clientA")
	}

	gotB, err := reg.Get("alias-b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotB != clientB {
		t.Error("expected clientB")
	}
}
