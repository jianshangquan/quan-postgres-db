package wal

import "testing"

func TestValidateID(t *testing.T) {
	ok := []string{"default", "main", "t1", "A_b-9"}
	for _, id := range ok {
		if err := ValidateID(id); err != nil {
			t.Fatalf("%q: %v", id, err)
		}
	}
	bad := []string{"../x", "a/b", ".", "..", "", "has space", string(make([]byte, 65))}
	for _, id := range bad {
		if err := ValidateID(id); err == nil {
			t.Fatalf("expected reject %q", id)
		}
	}
}
