//! Constants used in the mysql/mariadb protocol

/// Bit flags used for fields as described in
/// <https://mariadb.com/docs/server/reference/clientserver-protocol/4-server-response-packets/result-set-packets#field-details-flag>
pub mod column_flag {
    /// Field can't be `NULL`.
    pub const NOT_NULL: u16 = 1;
    /// Field is part of a primary key.
    pub const PRIMARY_KEY: u16 = 2;
    /// Field is part of a unique key.
    pub const UNIQUE_KEY: u16 = 4;
    /// Field is part of a multi-part unique or primary key.
    pub const MULTIPLE_KEY: u16 = 8;
    /// Field is a blob.
    pub const BLOB: u16 = 16;
    /// Field is unsigned.
    pub const UNSIGNED: u16 = 32;
    /// Field is zero filled.
    pub const ZEROFILL: u16 = 64;
    /// Field is binary.
    pub const BINARY: u16 = 128;
    /// Field is an enumeration.
    pub const ENUM: u16 = 256;
    /// Field is an auto-incement field.
    pub const AUTO_INCREMENT: u16 = 512;
    /// Field is a timestamp.
    pub const TIMESTAMP: u16 = 1024;
    /// Field is a set.
    pub const SET: u16 = 2048;
    /// Field does not have a default value.
    pub const NO_DEFAULT_VALUE: u16 = 4096;
    /// Field is set to NOW on UPDATE.
    pub const ON_UPDATE_NOW: u16 = 8192;
    /// Field is a number.
    pub const NUM: u16 = 32768;
}

/// Field types as described in
/// <https://mariadb.com/docs/server/reference/clientserver-protocol/4-server-response-packets/result-set-packets#field-types>
pub mod type_ {
    #![allow(missing_docs)]
    pub const DECIMAL: u8 = 0x00;
    pub const TINY: u8 = 0x01;
    pub const SHORT: u8 = 0x02;
    pub const LONG: u8 = 0x03;
    pub const FLOAT: u8 = 0x04;
    pub const DOUBLE: u8 = 0x05;
    pub const NULL: u8 = 0x06;
    pub const TIMESTAMP: u8 = 0x07;
    pub const LONG_LONG: u8 = 0x08;
    pub const INT24: u8 = 0x09;
    pub const DATE: u8 = 0x0a;
    pub const TIME: u8 = 0x0b;
    pub const DATETIME: u8 = 0x0c;
    pub const YEAR: u8 = 0x0d;
    pub const VAR_CHAR: u8 = 0x0f;
    pub const BIT: u8 = 0x10;
    pub const JSON: u8 = 0xf5;
    pub const NEW_DECIMAL: u8 = 0xf6;
    pub const ENUM: u8 = 0xf7;
    pub const SET: u8 = 0xf8;
    pub const TINY_BLOB: u8 = 0xf9;
    pub const MEDIUM_BLOB: u8 = 0xfa;
    pub const LONG_BLOB: u8 = 0xfb;
    pub const BLOB: u8 = 0xfc;
    pub const VAR_STRING: u8 = 0xfd;
    pub const STRING: u8 = 0xfe;
    pub const GEOMETRY: u8 = 0xff;
}

/// Client capability flags
pub(crate) mod client {
    #![allow(clippy::missing_docs_in_private_items)]
    #![allow(unused)]
    pub const LONG_PASSWORD: u32 = 1;
    pub const FOUND_ROWS: u32 = 2;
    pub const LONG_FLAG: u32 = 4;
    pub const CONNECT_WITH_DB: u32 = 8;
    pub const LOCAL_FILES: u32 = 128;
    pub const IGNORE_SPACE: u32 = 256;
    pub const PROTOCOL_41: u32 = 512;
    pub const INTERACTIVE: u32 = 1024;
    pub const TRANSACTIONS: u32 = 8192;
    pub const SECURE_CONNECTION: u32 = 1 << 15;
    pub const MULTI_STATEMENTS: u32 = 1 << 16;
    pub const MULTI_RESULTS: u32 = 1 << 17;
    pub const PS_MULTI_RESULTS: u32 = 1 << 18;
    pub const PLUGIN_AUTH: u32 = 1 << 19;
    pub const DEPRECATE_EOF: u32 = 1 << 24;
}

/// Package types
pub(crate) mod com {
    /// Execute statement, see <https://mariadb.com/docs/server/reference/clientserver-protocol/3-binary-protocol-prepared-statements/com_stmt_execute>
    pub const STMT_EXECUTE: u8 = 0x17;
    /// Close statement, see <https://mariadb.com/docs/server/reference/clientserver-protocol/3-binary-protocol-prepared-statements/3-binary-protocol-prepared-statements-com_stmt_close>
    pub const STMT_CLOSE: u8 = 0x19;
    /// Prepare statement, see <https://mariadb.com/docs/server/reference/clientserver-protocol/3-binary-protocol-prepared-statements/com_stmt_prepare>
    pub const STMT_PREPARE: u8 = 0x16;
    /// Query, see https://mariadb.com/docs/server/reference/clientserver-protocol/2-text-protocol/com_query
    pub const QUERY: u8 = 0x03;
    /// Ping, see https://mariadb.com/docs/server/reference/clientserver-protocol/2-text-protocol/com_ping
    pub const PING: u8 = 0x0E;
}
