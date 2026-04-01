package iceberg

// IcebergType represents an Iceberg type string used in metadata JSON.
type IcebergType = string

const (
	TypeBoolean     IcebergType = "boolean"
	TypeInt         IcebergType = "int"
	TypeLong        IcebergType = "long"
	TypeFloat       IcebergType = "float"
	TypeDouble      IcebergType = "double"
	TypeString      IcebergType = "string"
	TypeDate        IcebergType = "date"
	TypeTimestamp   IcebergType = "timestamp"
	TypeTimestampTZ IcebergType = "timestamptz"
	TypeUUID        IcebergType = "uuid"
	TypeBinary      IcebergType = "binary"
)

// PgOIDToIcebergType maps a Postgres OID to its Iceberg type string.
func PgOIDToIcebergType(oid uint32) IcebergType {
	switch oid {
	case 16: // bool
		return TypeBoolean
	case 21, 23: // int2, int4
		return TypeInt
	case 20: // int8
		return TypeLong
	case 700: // float4
		return TypeFloat
	case 701: // float8
		return TypeDouble
	case 1082: // date
		return TypeDate
	case 1114: // timestamp
		return TypeTimestamp
	case 1184: // timestamptz
		return TypeTimestampTZ
	case 2950: // uuid
		return TypeUUID
	case 17: // bytea
		return TypeBinary
	default:
		// text (25), varchar (1043), json (114), jsonb (3802), numeric (1700), etc.
		return TypeString
	}
}
