package iceberg

import "testing"

func TestPgOIDToIcebergType(t *testing.T) {
	tests := []struct {
		oid  uint32
		want IcebergType
	}{
		{16, TypeBoolean},
		{21, TypeInt},
		{23, TypeInt},
		{20, TypeLong},
		{700, TypeFloat},
		{701, TypeDouble},
		{25, TypeString},
		{1043, TypeString},
		{1082, TypeDate},
		{1114, TypeTimestamp},
		{1184, TypeTimestampTZ},
		{2950, TypeUUID},
		{17, TypeBinary},
		{114, TypeString},  // json
		{3802, TypeString}, // jsonb
		{1700, TypeString}, // numeric (string in Phase 1)
		{9999, TypeString}, // unknown → string fallback
	}

	for _, tt := range tests {
		got := PgOIDToIcebergType(tt.oid)
		if got != tt.want {
			t.Errorf("PgOIDToIcebergType(%d) = %q, want %q", tt.oid, got, tt.want)
		}
	}
}

func TestValidateTypeWidening(t *testing.T) {
	// Allowed widenings.
	if err := ValidateTypeWidening(TypeInt, TypeLong); err != nil {
		t.Errorf("int→long should be allowed: %v", err)
	}
	if err := ValidateTypeWidening(TypeFloat, TypeDouble); err != nil {
		t.Errorf("float→double should be allowed: %v", err)
	}
	// Same type is always OK.
	if err := ValidateTypeWidening(TypeString, TypeString); err != nil {
		t.Errorf("string→string should be allowed: %v", err)
	}

	// Incompatible changes should fail.
	if err := ValidateTypeWidening(TypeLong, TypeInt); err == nil {
		t.Error("long→int should be rejected")
	}
	if err := ValidateTypeWidening(TypeString, TypeInt); err == nil {
		t.Error("string→int should be rejected")
	}
	if err := ValidateTypeWidening(TypeInt, TypeBoolean); err == nil {
		t.Error("int→boolean should be rejected")
	}
}
