package memdb

import "testing"

func testValidSchema() *DBSchema {
	return &DBSchema{
		Tables: map[string]*TableSchema{
			"main": {
				Name: "main",
				Indexes: map[string]*IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &StringFieldIndex{Field: "ID"},
					},
					"foo": {
						Name:    "foo",
						Indexer: &StringFieldIndex{Field: "Foo"},
					},
					"qux": {
						Name:    "qux",
						Indexer: &StringSliceFieldIndex{Field: "Qux"},
					},
				},
			},
		},
	}
}

func TestDBSchema_Validate(t *testing.T) {
	s := &DBSchema{}
	err := s.Validate()
	if err == nil {
		t.Fatalf("should not validate, empty")
	}

	s.Tables = map[string]*TableSchema{
		"foo": {Name: "foo"},
	}
	err = s.Validate()
	if err == nil {
		t.Fatalf("should not validate, no indexes")
	}

	valid := testValidSchema()
	err = valid.Validate()
	if err != nil {
		t.Fatalf("should validate: %v", err)
	}
}

func TestTableSchema_Validate(t *testing.T) {
	s := &TableSchema{}
	err := s.Validate()
	if err == nil {
		t.Fatalf("should not validate, empty")
	}

	s.Indexes = map[string]*IndexSchema{
		"foo": {Name: "foo"},
	}
	err = s.Validate()
	if err == nil {
		t.Fatalf("should not validate, no indexes")
	}

	valid := &TableSchema{
		Name: "main",
		Indexes: map[string]*IndexSchema{
			"id": {
				Name:    "id",
				Unique:  true,
				Indexer: &StringFieldIndex{Field: "ID", Lowercase: true},
			},
		},
	}
	err = valid.Validate()
	if err != nil {
		t.Fatalf("should validate: %v", err)
	}
}

func TestIndexSchema_Validate(t *testing.T) {
	s := &IndexSchema{}
	err := s.Validate()
	if err == nil {
		t.Fatalf("should not validate, empty")
	}

	s.Name = "foo"
	err = s.Validate()
	if err == nil {
		t.Fatalf("should not validate, no indexer")
	}

	s.Indexer = &StringFieldIndex{Field: "Foo", Lowercase: false}
	err = s.Validate()
	if err != nil {
		t.Fatalf("should validate: %v", err)
	}

	s.Indexer = &StringSliceFieldIndex{Field: "Qux", Lowercase: false}
	err = s.Validate()
	if err != nil {
		t.Fatalf("should validate: %v", err)
	}
}
