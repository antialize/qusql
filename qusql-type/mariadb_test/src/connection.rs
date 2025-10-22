use crate::error::{Context, Error, Result};
use std::{
    io::{Read, Write},
    net::TcpStream,
};

#[derive(Debug)]
pub enum MariaDBType {
    Decimal,
    Tiny,
    Short,
    Long,
    Float,
    Double,
    Null,
    Timestamp,
    LongLong,
    Iin24,
    Date,
    Time,
    Datetime,
    Yeay,
    VarChar,
    Bit,
    Json,
    NewDicimal,
    Enum,
    Set,
    TinyBlob,
    MediumBlob,
    LongBlob,
    Blob,
    VarString,
    String,
    Geometry,
}

/// Read mysql encoded packages
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn u8(&mut self) -> Result<u8> {
        Ok(*self.0.split_off_first().context("u8")?)
    }

    fn u16(&mut self) -> Result<u16> {
        let p = self.0.split_off(..2).context("u16")?;
        Ok(u16::from_le_bytes(p.try_into().unwrap()))
    }

    fn u24(&mut self) -> Result<u32> {
        Err(Error::bail("u24 not implemented"))
    }

    fn u32(&mut self) -> Result<u32> {
        let p = self.0.split_off(..4).context("u32")?;
        Ok(u32::from_le_bytes(p.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        let p = self.0.split_off(..8).context("u64")?;
        Ok(u64::from_le_bytes(p.try_into().unwrap()))
    }

    fn lenenc(&mut self) -> Result<u64> {
        let v = self.u8()?;
        Ok(match v {
            0xFC => self.u16()?.into(),
            0xFD => self.u24()?.into(),
            0xFE => self.u64()?,
            v => v.into(),
        })
    }

    fn str(&mut self) -> Result<&'a str> {
        let len = self.lenenc()? as usize;
        let v = self.0.split_off(..len).context("str")?;
        Ok(str::from_utf8(v)?)
    }
}

#[derive(Debug)]
pub struct ColumnDef {
    pub column: String,
    pub field_type: MariaDBType,
    pub not_null: bool,
    pub unsigned: bool,
}

#[derive(Debug)]
pub enum TypeStatementResult {
    Typed {
        params: Vec<ColumnDef>,
        columns: Vec<ColumnDef>,
    },
    Error {
        code: u16,
        message: String,
    },
}

/// Establish a mariadb connection, the protocol is not that complicated so we just implement it here
/// instead of having external dependencies
pub struct Connection(TcpStream);

impl Connection {
    fn read_pkg(&mut self) -> Result<Vec<u8>> {
        let mut size = [0; 4];
        self.0.read_exact(&mut size)?;
        let y = u32::from_le_bytes(size);
        let len: usize = (y & 0xFFFFFF).try_into()?;
        if len == 0xFFFFFF {
            return Err(Error::bail("Long packages not implemented"));
        }
        let mut r = vec![0; len];
        self.0.read_exact(&mut r)?;
        Ok(r)
    }

    fn send_pkg(&mut self, pkg: &[u8], seq: u8) -> Result<()> {
        let l: u32 = pkg.len().try_into()?;
        let a = l + (u32::from(seq) << 24);
        self.0.write_all(&a.to_le_bytes())?;
        self.0.write_all(pkg)?;
        self.0.flush()?;
        Ok(())
    }

    fn read_column_def(&mut self) -> Result<ColumnDef> {
        let p = self.read_pkg()?;
        let mut r = Reader(&p);
        if r.str()? != "def" {
            return Err(Error::bail("Expected def"));
        }
        let _schema = r.str()?;
        let _table_alias = r.str()?;
        let _table = r.str()?;
        let column_alias = r.str()?;
        let _column = r.str()?;
        let _lff = r.lenenc()?;
        let _char_set = r.u16()?;
        let _max_col_size = r.u32()?;
        let field_type = r.u8()?;
        let field_details = r.u8()?;
        let _decimals = r.u8()?;
        let _unused = r.u16()?;

        const TYPE_DECIMAL: u8 = 0x00;
        const TYPE_TINY: u8 = 0x01;
        const TYPE_SHORT: u8 = 0x02;
        const TYPE_LONG: u8 = 0x03;
        const TYPE_FLOAT: u8 = 0x04;
        const TYPE_DOUBLE: u8 = 0x05;
        const TYPE_NULL: u8 = 0x06;
        const TYPE_TIMESTAMP: u8 = 0x07;
        const TYPE_LONG_LONG: u8 = 0x08;
        const TYPE_INT24: u8 = 0x09;
        const TYPE_DATE: u8 = 0x0a;
        const TYPE_TIME: u8 = 0x0b;
        const TYPE_DATETIME: u8 = 0x0c;
        const TYPE_YEAR: u8 = 0x0d;
        const TYPE_VAR_CHAR: u8 = 0x0f;
        const TYPE_BIT: u8 = 0x10;
        const TYPE_JSON: u8 = 0xf5;
        const TYPE_NEW_DECIMAL: u8 = 0xf6;
        const TYPE_ENUM: u8 = 0xf7;
        const TYPE_SET: u8 = 0xf8;
        const TYPE_TINY_BLOB: u8 = 0xf9;
        const TYPE_MEDIUM_BLOB: u8 = 0xfa;
        const TYPE_LONG_BLOB: u8 = 0xfb;
        const TYPE_BLOB: u8 = 0xfc;
        const TYPE_VAR_STRING: u8 = 0xfd;
        const TYPE_STRING: u8 = 0xfe;
        const TYPE_GEOMETRY: u8 = 0xff;
        let field_type = match field_type {
            TYPE_DECIMAL => MariaDBType::Decimal,
            TYPE_TINY => MariaDBType::Tiny,
            TYPE_SHORT => MariaDBType::Short,
            TYPE_LONG => MariaDBType::Long,
            TYPE_FLOAT => MariaDBType::Float,
            TYPE_DOUBLE => MariaDBType::Double,
            TYPE_NULL => MariaDBType::Null,
            TYPE_TIMESTAMP => MariaDBType::Timestamp,
            TYPE_LONG_LONG => MariaDBType::LongLong,
            TYPE_INT24 => MariaDBType::Iin24,
            TYPE_DATE => MariaDBType::Date,
            TYPE_TIME => MariaDBType::Time,
            TYPE_DATETIME => MariaDBType::Datetime,
            TYPE_YEAR => MariaDBType::Yeay,
            TYPE_VAR_CHAR => MariaDBType::VarChar,
            TYPE_BIT => MariaDBType::Bit,
            TYPE_JSON => MariaDBType::Json,
            TYPE_NEW_DECIMAL => MariaDBType::NewDicimal,
            TYPE_ENUM => MariaDBType::Enum,
            TYPE_SET => MariaDBType::Set,
            TYPE_TINY_BLOB => MariaDBType::TinyBlob,
            TYPE_MEDIUM_BLOB => MariaDBType::MediumBlob,
            TYPE_LONG_BLOB => MariaDBType::LongBlob,
            TYPE_BLOB => MariaDBType::Blob,
            TYPE_VAR_STRING => MariaDBType::VarString,
            TYPE_STRING => MariaDBType::String,
            TYPE_GEOMETRY => MariaDBType::Geometry,
            _ => return Err(Error::bail(format!("Unhandled type {field_type}"))),
        };

        const COLUMN_FLAG_NOT_NULL: u8 = 1;
        const COLUMN_FLAG_UNSIGNED: u8 = 32;
        let not_null = (field_details & COLUMN_FLAG_NOT_NULL) != 0;
        let unsigned = (field_details & COLUMN_FLAG_UNSIGNED) != 0;

        Ok(ColumnDef {
            column: column_alias.to_string(),
            field_type,
            not_null,
            unsigned,
        })
    }

    /// Prepare statement to get type information
    /// then free prepared statement
    pub fn type_statement(&mut self, stmt: &str) -> Result<TypeStatementResult> {
        const COM_STMT_CLOSE: u8 = 0x19;
        const COM_STMT_PREPARE: u8 = 0x16;

        let mut p = Vec::with_capacity(1 + stmt.len());
        p.push(COM_STMT_PREPARE);
        p.extend_from_slice(stmt.as_bytes());
        self.send_pkg(&p, 0).context("Sending prepare statement")?;
        let r = self.read_pkg()?;
        let mut r = Reader(&r);

        match r.u8()? {
            0 => {
                let stmt_id = r.u32()?;
                let col_count = r.u16()?;
                let param_count = r.u16()?;
                let mut params = Vec::with_capacity(param_count as usize);
                for _ in 0..param_count {
                    params.push(self.read_column_def()?)
                }
                let mut columns = Vec::with_capacity(param_count as usize);
                for _ in 0..col_count {
                    columns.push(self.read_column_def()?)
                }
                p.clear();
                p.push(COM_STMT_CLOSE);
                p.extend_from_slice(&stmt_id.to_le_bytes());
                self.send_pkg(&p, 0).context("Close stmt")?;
                Ok(TypeStatementResult::Typed { params, columns })
            }
            0xff => {
                let code = r.u16()?;
                if code == 0xffff {
                    return Err(Error::bail("Unhandled ffff code"));
                }
                let message = str::from_utf8(r.0)?.to_string();
                Ok(TypeStatementResult::Error { code, message })
            }
            r => {
                Err(Error::bail(format!(
                    "Expected prepare statement response {r}"
                )))
            }
        }
    }

    /// Connect to mariadb
    pub fn connect() -> Result<Self> {
        const CLIENT_LONG_FLAG: u32 = 4;
        const CLIENT_CONNECT_WITH_DB: u32 = 8;
        const CLIENT_PROTOCOL_41: u32 = 512;
        const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

        let mut conn = Self(TcpStream::connect("127.0.0.1:3367").context("Connecting to mariadb")?);

        let head = conn.read_pkg().context("Reading head package")?;
        if !matches!(head.first(), Some(10)) {
            return Err(Error::bail("Invalid head package"));
        }

        let mut ack = Vec::new();
        ack.extend_from_slice(&u32::to_le_bytes(
            CLIENT_LONG_FLAG | CLIENT_CONNECT_WITH_DB | CLIENT_PROTOCOL_41 | CLIENT_DEPRECATE_EOF,
        ));
        ack.extend_from_slice(&u32::to_le_bytes(0x1000000));
        ack.push(45); //utf8mb4_general_ci
        ack.extend(&[0;23]);
        ack.extend_from_slice("root\0".as_bytes());
        // No password
        ack.push(0);
        ack.extend_from_slice("db\0".as_bytes());
        conn.send_pkg(&ack, 1)?;

        let synack = conn.read_pkg()?;
        if !matches!(synack.first(), Some(0)) {
            return Err(Error::bail("Invalid synack package"));
        }
        Ok(conn)
    }
}
