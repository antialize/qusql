//! Handle a single connection to Mariadb/Mysql
use std::{
    borrow::Cow,
    collections::{HashMap, hash_map::Entry},
    fmt::Display,
    marker::PhantomData,
    mem::ManuallyDrop,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::Range,
};

use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpSocket,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};

use crate::{
    args::Args,
    auth::compute_auth,
    bind::{Bind, BindError},
    constants::{client, com},
    decode::Column,
    package_parser::{DecodeError, DecodeResult, PackageParser},
    row::{FromRow, Row},
};

/// Error handling connection
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ConnectionErrorContent {
    /// An error returned by Mariadb/Mysql
    #[error("mysql error {code}: {message}")]
    Mysql {
        /// 2-byte mysql error code
        code: u16,
        /// 5-byte mysql status,
        status: [u8; 5],
        /// Error message
        message: String,
    },
    /// Network error from tokio
    #[error(transparent)]
    Io(#[from] tokio::io::Error),
    /// Error decoding row
    #[error("error reading {0}: {1}")]
    Decode(&'static str, DecodeError),
    /// Error binding parameter
    #[error("error binding paramater {0}: {1}")]
    Bind(u16, BindError),
    /// Mariadb/Mysql did not speak the protocol correctly
    #[error("protocol error {0}")]
    ProtocolError(String),
    /// You executed a mysql statement that does not return any columns
    #[error("fetch return no columns")]
    ExpectedRows,
    /// You executed a mysql statement that does return columns, so you need to read the rowss
    #[error("rows return for execute")]
    UnexpectedRows,
    #[cfg(feature = "cancel_testing")]
    /// For testing cancel safety
    #[doc(hidden)]
    #[error("await threshold reached")]
    TestCancelled,
}

/// Error handling connection
///
/// This types is a Box around ErrorContent, to make sure
/// that the error type is as small as possible
pub struct ConnectionError(Box<ConnectionErrorContent>);

const _: () = {
    assert!(size_of::<ConnectionError>() == size_of::<usize>());
};

impl ConnectionError {
    /// Return the content of the error
    pub fn content(&self) -> &ConnectionErrorContent {
        &self.0
    }
}

impl std::ops::Deref for ConnectionError {
    type Target = ConnectionErrorContent;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<E: Into<ConnectionErrorContent>> From<E> for ConnectionError {
    fn from(value: E) -> Self {
        ConnectionError(Box::new(value.into()))
    }
}

impl std::fmt::Debug for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for ConnectionError {}

/// Result return by the connection
pub type ConnectionResult<T> = std::result::Result<T, ConnectionError>;
/// Convert [crate::package_parser::DecodeError] into [ConnectionError::Decode] with an attached location
pub trait WithLoc<T> {
    /// Convert [crate::package_parser::DecodeError] into [ConnectionError::Decode] with an attached location
    fn loc(self, loc: &'static str) -> ConnectionResult<T>;
}

impl<T> WithLoc<T> for DecodeResult<T> {
    fn loc(self, loc: &'static str) -> ConnectionResult<T> {
        self.map_err(|v| ConnectionErrorContent::Decode(loc, v).into())
    }
}

/// Trait used to check expected values
trait Except {
    /// The values to expect
    type Value;

    /// Check that I have the expected value
    fn ev(self, loc: &'static str, expected: Self::Value) -> ConnectionResult<()>;
}

impl<T: Eq + Display> Except for DecodeResult<T> {
    type Value = T;

    fn ev(self, loc: &'static str, expected: T) -> ConnectionResult<()> {
        let v = self.loc(loc)?;
        if v != expected {
            Err(ConnectionErrorContent::ProtocolError(format!(
                "Expected {expected} for {loc} got {v}"
            ))
            .into())
        } else {
            Ok(())
        }
    }
}

/// Map a row into some concrete type
pub trait RowMap: Send {
    /// The error type returned by map
    type E: From<ConnectionError> + Send;

    /// The value type returned by map
    type T<'a>: Send;

    /// Map the row into a concrete type
    fn map<'a>(row: Row<'a>) -> Result<Self::T<'a>, Self::E>;
}

/// Reader used to read packages from Mariadb/Mysql
struct Reader {
    /// Buffer used to contain one or more packages
    buff: BytesMut,
    /// Socket to read from
    read: OwnedReadHalf,
    /// Number of bytes to skip on next read
    skip_on_read: usize,
    /// Keep old packages, do not advance the buffer
    buffer_packages: bool,
}

impl Reader {
    /// Construct a new reader instance
    fn new(read: OwnedReadHalf) -> Self {
        Self {
            read,
            buff: BytesMut::with_capacity(1234),
            skip_on_read: 0,
            buffer_packages: false,
        }
    }

    /// Read next package, into buff and return the byte ranges in the buffer
    ///
    /// The returned future is cancel-safe
    async fn read_raw(&mut self) -> ConnectionResult<Range<usize>> {
        if !self.buffer_packages {
            self.buff.advance(self.skip_on_read);
            self.skip_on_read = 0;
        }

        while self.buff.remaining() < 4 + self.skip_on_read {
            self.read.read_buf(&mut self.buff).await?;
        }
        let y: u32 = u32::from_le_bytes(
            self.buff[self.skip_on_read..self.skip_on_read + 4]
                .try_into()
                .unwrap(),
        );
        let len: usize = (y & 0xFFFFFF).try_into().unwrap();
        let _s = (y >> 24) as u8;
        if len == 0xFFFFFF {
            return Err(ConnectionErrorContent::ProtocolError(
                "Extended packages not supported".to_string(),
            )
            .into());
        }
        while self.buff.remaining() < self.skip_on_read + 4 + len {
            self.read.read_buf(&mut self.buff).await?;
        }
        let r = self.skip_on_read + 4..self.skip_on_read + 4 + len;
        self.skip_on_read += 4 + len;
        Ok(r)
    }

    /// Read next package, into buff and return its data
    ///
    /// The returned future is cancel-safe
    #[inline]
    async fn read(&mut self) -> ConnectionResult<&[u8]> {
        let r = self.read_raw().await?;
        Ok(self.bytes(r))
    }

    /// Return the bytes for a given range.
    ///
    /// The range should have been returned by a previous read_raw
    ///
    /// [Self::buffer_packages] must have been set to true for any
    /// reads in between the read call that return r and this call
    /// to bytes for the returned bytes to make sense
    #[inline]
    fn bytes(&self, r: Range<usize>) -> &[u8] {
        &self.buff[r]
    }
}

/// Writer used to send packages to Mariadb/Mysql
struct Writer {
    /// Writer to write package to
    write: OwnedWriteHalf,
    /// Buffer containing package
    buff: BytesMut,
    /// Sequence number of package
    seq: u8,
}

impl Writer {
    /// Construct a new [Writer] instance
    fn new(write: OwnedWriteHalf) -> Self {
        Writer {
            write,
            buff: BytesMut::with_capacity(1234),
            seq: 1,
        }
    }

    /// Compose a new package
    fn compose(&mut self) -> Composer<'_> {
        self.buff.clear();
        self.buff.put_u32(0);
        Composer { writer: self }
    }

    /// Send the last composed package
    async fn send(&mut self) -> ConnectionResult<()> {
        Ok(self.write.write_all_buf(&mut self.buff).await?)
    }
}

/// Struct used to compose a singe network package in a [Writer]
struct Composer<'a> {
    /// Writer to compose package into
    writer: &'a mut Writer,
}

impl<'a> Composer<'a> {
    /// Write a u32 to the package
    fn put_u32(&mut self, v: u32) {
        self.writer.buff.put_u32_le(v)
    }

    /// Write a u16 to the package
    fn put_u16(&mut self, v: u16) {
        self.writer.buff.put_u16_le(v)
    }

    /// Write a u8 to the package
    fn put_u8(&mut self, v: u8) {
        self.writer.buff.put_u8(v)
    }

    /// Write a null terminated string to the package
    fn put_str_null(&mut self, s: &str) {
        self.writer.buff.put(s.as_bytes());
        self.writer.buff.put_u8(0);
    }

    /// Write write some bytes to the package
    fn put_bytes(&mut self, s: &[u8]) {
        self.writer.buff.put(s);
    }

    /// Finalize the package header
    fn finalize(self) {
        let len = self.writer.buff.len();
        let mut x = &mut self.writer.buff[..4];
        x.put_u32_le((len - 4) as u32 | ((self.writer.seq as u32) << 24));
        self.writer.seq = self.writer.seq.wrapping_add(1);
    }
}

/// Options used to establish connection to Mariadb/Mysql
pub struct ConnectionOptions<'a> {
    /// The TCP adders to connect to
    pub address: SocketAddr,
    /// The user to connect as
    pub user: Cow<'a, str>,
    /// The password for the user
    pub password: Cow<'a, str>,
    /// The database to connect to
    pub database: Cow<'a, str>,
}

impl<'a> Default for ConnectionOptions<'a> {
    fn default() -> Self {
        Self {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4407),
            user: Cow::Borrowed("root"),
            password: Cow::Borrowed("password"),
            database: Cow::Borrowed("db"),
        }
    }
}

/// A prepared statement
struct Statement {
    /// The id of the statement
    stmt_id: u32,
    /// The number of parameters the statement takes
    num_params: u16,
}

/// Iterate over rows in a query result
pub struct QueryIterator<'a> {
    /// A reference to the connection
    connection: &'a mut RawConnection,
}

impl<'a> QueryIterator<'a> {
    /// Fetch the next row from the result set
    ///
    /// The returned future is cancel-safe.
    pub async fn next(&mut self) -> ConnectionResult<Option<Row<'_>>> {
        match self.connection.state {
            ConnectionState::Clean => return Ok(None),
            ConnectionState::QueryReadRows => (),
            _ => panic!("Logic error"),
        }
        // safety-cancel: The cleanup on the connection will skip the remaining rows
        self.connection.test_cancel()?;
        let package = self.connection.reader.read().await?;
        let mut pp = PackageParser::new(package);
        match pp.get_u8().loc("Row first byte")? {
            0x00 => Ok(Some(Row::new(&self.connection.columns, package))),
            0xFE => {
                //EOD
                self.connection.state = ConnectionState::Clean;
                Ok(None)
            }
            0xFF => {
                handle_mysql_error(&mut pp)?;
                unreachable!()
            }
            v => Err(ConnectionErrorContent::ProtocolError(format!(
                "Unexpected response type {v} to row package"
            ))
            .into()),
        }
    }
}

/// Iterate over mapped rows in a query result
pub struct MapQueryIterator<'a, M: RowMap> {
    /// A reference to the connection
    connection: &'a mut RawConnection,
    /// We need a phantom data fro M
    _phantom: PhantomData<M>,
}

impl<'a, M: RowMap> MapQueryIterator<'a, M> {
    /// Fetch the next row from the result mapped using M
    ///
    /// The returned future is cancel-safe.
    pub async fn next(&mut self) -> Result<Option<M::T<'_>>, M::E> {
        match self.connection.state {
            ConnectionState::Clean => return Ok(None),
            ConnectionState::QueryReadRows => (),
            _ => panic!("Logic error"),
        }
        // safety-cancel: The cleanup on the connection will skip the remaining rows
        self.connection.test_cancel()?;
        let package = self.connection.reader.read().await?;
        let mut pp = PackageParser::new(package);
        match pp.get_u8().loc("Row first byte")? {
            0x00 => Ok(Some(M::map(Row::new(&self.connection.columns, package))?)),
            0xFE => {
                //EOD
                self.connection.state = ConnectionState::Clean;
                Ok(None)
            }
            0xFF => {
                handle_mysql_error(&mut pp)?;
                unreachable!()
            }
            v => Err(
                ConnectionError::from(ConnectionErrorContent::ProtocolError(format!(
                    "Unexpected response type {v} to row package"
                )))
                .into(),
            ),
        }
    }
}

/// The result of a execute
pub struct ExecuteResult {
    /// The number of rows affected by the query
    pub affected_rows: u64,
    /// The id of the last row inserted
    pub last_insert_id: u64,
}

/// The result of a query
enum QueryResult {
    /// The query returned columns
    WithColumns,
    /// The query returns no columns
    ExecuteResult(ExecuteResult),
}

/// Internal state of the connection. used for cleanup.
#[derive(Clone, Copy, Debug)]
enum ConnectionState {
    /// The connection is in a clean state
    Clean,
    /// We are sending the prepare statement package
    PrepareStatementSend,
    /// We are reading the prepare statement response
    PrepareStatementReadHead,
    /// We are reading the prepare statement parameters
    PrepareStatementReadParams {
        /// Number of params left to read
        params: u16,
        /// Number of columns left to read
        columns: u16,
        /// The id of the prepared statement
        stmt_id: u32,
    },
    /// We are sending a close prepared statement package
    ClosePreparedStatement,
    /// We are sending a query
    QuerySend,
    /// We are reading a query response
    QueryReadHead,
    /// We are reading query columns
    QueryReadColumns(u64),
    /// We are reading query rows
    QueryReadRows,
    /// We are sending an unprepared statement
    UnpreparedSend,
    /// We are receiving the response of an unprepared statement
    UnpreparedRecv,
    /// The connection is in a broken state and cannot be recovered
    Broken,
}

/// A raw connection to Mariadb/Mysql. This is split from [Connection] to that connection
/// can handle the caching of prepared statement
struct RawConnection {
    /// The reader to read packages from
    reader: Reader,
    /// The writer to write packages to
    writer: Writer,
    /// The current state of the connection
    state: ConnectionState,
    /// Buffer to store column information
    columns: Vec<Column>,
    /// Ranges used by fetch all
    ranges: Vec<Range<usize>>,
    #[cfg(feature = "cancel_testing")]
    /// Return TestCancelled after this many sends
    cancel_count: Option<usize>,
}

/// Parse a column definition package
fn parse_column_definition(p: &mut PackageParser) -> ConnectionResult<Column> {
    p.skip_lenenc_str().loc("catalog")?;
    p.skip_lenenc_str().loc("schema")?;
    p.skip_lenenc_str().loc("table")?;
    p.skip_lenenc_str().loc("org_table")?;
    p.skip_lenenc_str().loc("name")?;
    p.skip_lenenc_str().loc("org_name")?;
    p.get_lenenc().loc("length of fixed length fields")?;
    let character_set = p.get_u16().loc("character_set")?;
    p.get_u32().loc("column_length")?;
    let r#type = p.get_u8().loc("type")?;
    let flags = p.get_u16().loc("flags")?;
    p.get_u8().loc("decimals")?;
    p.get_u16().loc("res")?;
    Ok(Column {
        r#type,
        flags,
        character_set,
    })
}

/// Parse an error package
fn handle_mysql_error(pp: &mut PackageParser) -> ConnectionResult<std::convert::Infallible> {
    // We assume the 255 has been read
    let code = pp.get_u16().loc("code")?;
    pp.get_u8().ev("sharp", b'#')?;
    let a = pp.get_u8().loc("status0")?;
    let b = pp.get_u8().loc("status1")?;
    let c = pp.get_u8().loc("status2")?;
    let d = pp.get_u8().loc("status3")?;
    let e = pp.get_u8().loc("status4")?;
    let msg = pp.get_eof_str().loc("message")?;
    Err(ConnectionErrorContent::Mysql {
        code,
        status: [a, b, c, d, e],
        message: msg.to_string(),
    }
    .into())
}

/// Compute string to begin transaction
fn begin_transaction_query(depth: usize) -> Cow<'static, str> {
    match depth {
        0 => "BEGIN".into(),
        1 => "SAVEPOINT _sqly_savepoint_1".into(),
        2 => "SAVEPOINT _sqly_savepoint_2".into(),
        3 => "SAVEPOINT _sqly_savepoint_3".into(),
        v => format!("SAVEPOINT _sqly_savepoint_{}", v).into(),
    }
}

/// Compute string to commit transaction
fn commit_transaction_query(depth: usize) -> Cow<'static, str> {
    match depth {
        0 => "COMMIT".into(),
        1 => "RELEASE SAVEPOINT _sqly_savepoint_1".into(),
        2 => "RELEASE SAVEPOINT _sqly_savepoint_2".into(),
        3 => "RELEASE SAVEPOINT _sqly_savepoint_3".into(),
        v => format!("RELEASE SAVEPOINT _sqly_savepoint_{}", v).into(),
    }
}

/// Compute string to rollback transaction
fn rollback_transaction_query(depth: usize) -> Cow<'static, str> {
    match depth {
        0 => "ROLLBACK".into(),
        1 => "ROLLBACK TO SAVEPOINT _sqly_savepoint_1".into(),
        2 => "ROLLBACK TO SAVEPOINT _sqly_savepoint_2".into(),
        3 => "ROLLBACK TO SAVEPOINT _sqly_savepoint_3".into(),
        v => format!("RELEASE TO SAVEPOINT _sqly_savepoint_{}", v).into(),
    }
}

impl RawConnection {
    /// Connect to Mariadb/Mysql
    ///
    /// The returned future is cancel safe
    async fn connect(options: &ConnectionOptions<'_>) -> ConnectionResult<Self> {
        // safety-cancel: It is safe to drop this future since it does not mute shared state

        // Connect to socket
        let socket = TcpSocket::new_v4()?;
        let stream = socket.connect(options.address).await?;
        let (read, write) = stream.into_split();

        let mut reader = Reader::new(read);
        let mut writer = Writer::new(write);

        // Read and parse handshake package
        let package = reader.read().await?;
        let mut p = PackageParser::new(package);
        p.get_u8().ev("protocol version", 10)?;
        p.skip_null_str().loc("status")?;
        let _wthread_id = p.get_u32().loc("thread_id")?;
        let nonce1 = p.get_bytes(8).loc("nonce1")?;
        p.get_u8().ev("nonce1_end", 0)?;
        let capability_flags_1 = p.get_u16().loc("capability_flags_1")?;
        let _character_set = p.get_u8().loc("character_set")?;
        p.get_u16().loc("status_flags")?;
        let capability_flags_2 = p.get_u16().loc("capability_flags_2")?;
        let auth_plugin_data_len = p.get_u8().loc("auth_plugin_data_len")?;
        let _capability_flags = capability_flags_1 as u32 | (capability_flags_2 as u32) << 16;
        p.get_bytes(10).loc("reserved")?;
        let nonce2 = p
            .get_bytes(auth_plugin_data_len as usize - 9)
            .loc("nonce2")?;
        p.get_u8().ev("nonce2_end", 0)?;
        p.get_null_str()
            .ev("auth_plugin", "mysql_native_password")?;

        // Compose and send handshake response
        let mut p = writer.compose();
        p.put_u32(
            client::LONG_PASSWORD
                | client::LONG_FLAG
                | client::CONNECT_WITH_DB
                | client::LOCAL_FILES
                | client::PROTOCOL_41
                | client::DEPRECATE_EOF
                | client::TRANSACTIONS
                | client::SECURE_CONNECTION
                | client::MULTI_STATEMENTS
                | client::MULTI_RESULTS
                | client::PS_MULTI_RESULTS
                | client::PLUGIN_AUTH,
        );
        p.put_u32(0x1000000); // Max package size
        p.put_u16(45); //utf8mb4_general_ci
        for _ in 0..22 {
            p.put_u8(0);
        }
        p.put_str_null(&options.user);
        let mut auth = [0; 20];
        compute_auth(&options.password, nonce1, nonce2, &mut auth);
        p.put_u8(auth.len() as u8);
        for v in auth {
            p.put_u8(v);
        }
        p.put_str_null(&options.database);
        // mysql_native_password
        p.put_str_null("mysql_native_password");
        p.finalize();

        writer.send().await?;

        let p = reader.read().await?;
        let mut pp = PackageParser::new(p);
        match pp.get_u8().loc("response type")? {
            0xFF => {
                handle_mysql_error(&mut pp)?;
            }
            0x00 => {
                let _rows = pp.get_lenenc().loc("rows")?;
                let _last_inserted_id = pp.get_lenenc().loc("last_inserted_id")?;
            }
            v => {
                return Err(ConnectionErrorContent::ProtocolError(format!(
                    "Unexpected response type {v} to handshake response"
                ))
                .into());
            }
        }
        writer.seq = 0;
        Ok(RawConnection {
            reader,
            writer,
            state: ConnectionState::Clean,
            columns: Vec::new(),
            ranges: Vec::new(),
            #[cfg(feature = "cancel_testing")]
            cancel_count: None,
        })
    }

    /// Can be called self.cancel_count times
    /// before it returns Err(ConnectionError::TestCancelled)
    ///
    /// This is used to to test that that we can properly recover
    /// from dropped futures
    #[inline]
    fn test_cancel(&mut self) -> ConnectionResult<()> {
        #[cfg(feature = "cancel_testing")]
        if let Some(v) = &mut self.cancel_count {
            if *v == 0 {
                return Err(ConnectionErrorContent::TestCancelled.into());
            }
            *v -= 1;
        }
        Ok(())
    }

    /// Cleanup The connection if it is dirty
    async fn cleanup(&mut self) -> ConnectionResult<()> {
        loop {
            match self.state {
                ConnectionState::Clean => break,
                ConnectionState::PrepareStatementSend => {
                    self.test_cancel()?;
                    self.writer.send().await?;
                    self.state = ConnectionState::PrepareStatementReadHead;
                    continue;
                }
                ConnectionState::PrepareStatementReadHead => {
                    self.test_cancel()?;
                    let package = self.reader.read().await?;
                    let mut p = PackageParser::new(package);
                    match p.get_u8().loc("response type")? {
                        0 => {
                            let stmt_id = p.get_u32().loc("stmt_id")?;
                            let columns = p.get_u16().loc("num_columns")?;
                            let params = p.get_u16().loc("num_params")?;
                            self.state = ConnectionState::PrepareStatementReadParams {
                                params,
                                columns,
                                stmt_id,
                            };
                            continue;
                        }
                        255 => {
                            self.state = ConnectionState::Clean;
                        }
                        v => {
                            self.state = ConnectionState::Broken;
                            return Err(ConnectionErrorContent::ProtocolError(format!(
                                "Unexpected response type {v} to prepare statement"
                            ))
                            .into());
                        }
                    }
                }
                ConnectionState::PrepareStatementReadParams {
                    params: 0,
                    columns: 0,
                    stmt_id,
                } => {
                    self.writer.seq = 0;
                    let mut p = self.writer.compose();
                    p.put_u8(com::STMT_CLOSE);
                    p.put_u32(stmt_id);
                    p.finalize();
                    self.state = ConnectionState::ClosePreparedStatement;
                }
                ConnectionState::PrepareStatementReadParams {
                    params: 0,
                    columns,
                    stmt_id,
                } => {
                    self.test_cancel()?;
                    self.reader.read().await?;
                    self.state = ConnectionState::PrepareStatementReadParams {
                        params: 0,
                        columns: columns - 1,
                        stmt_id,
                    };
                }
                ConnectionState::PrepareStatementReadParams {
                    params,
                    columns,
                    stmt_id,
                } => {
                    self.test_cancel()?;
                    self.reader.read().await?;
                    self.state = ConnectionState::PrepareStatementReadParams {
                        params: params - 1,
                        columns,
                        stmt_id,
                    };
                }
                ConnectionState::ClosePreparedStatement => {
                    self.test_cancel()?;
                    self.writer.send().await?;
                    self.state = ConnectionState::Clean;
                }
                ConnectionState::QuerySend => {
                    self.test_cancel()?;
                    self.writer.send().await?;
                    self.state = ConnectionState::QueryReadHead;
                }
                ConnectionState::QueryReadHead => {
                    self.test_cancel()?;
                    let package = self.reader.read().await?;
                    {
                        let mut pp = PackageParser::new(package);
                        match pp.get_u8().loc("first_byte")? {
                            255 | 0 => {
                                self.state = ConnectionState::Clean;
                                continue;
                            }
                            _ => (),
                        }
                    }
                    let column_count = PackageParser::new(package)
                        .get_lenenc()
                        .loc("column_count")?;
                    self.state = ConnectionState::QueryReadColumns(column_count)
                }
                ConnectionState::QueryReadColumns(0) => {
                    self.state = ConnectionState::QueryReadRows;
                }
                ConnectionState::QueryReadColumns(cnt) => {
                    self.test_cancel()?;
                    self.reader.read().await?;
                    self.state = ConnectionState::QueryReadColumns(cnt - 1);
                }
                ConnectionState::QueryReadRows => {
                    self.test_cancel()?;
                    let package = self.reader.read().await?;
                    let mut pp = PackageParser::new(package);
                    match pp.get_u8().loc("Row first byte")? {
                        0x00 => (),
                        0xFE => {
                            //EOD
                            self.state = ConnectionState::Clean;
                        }
                        0xFF => {
                            self.state = ConnectionState::Broken;
                            handle_mysql_error(&mut pp)?;
                            unreachable!()
                        }
                        v => {
                            self.state = ConnectionState::Broken;
                            return Err(ConnectionErrorContent::ProtocolError(format!(
                                "Unexpected response type {v} to row package"
                            ))
                            .into());
                        }
                    }
                }
                ConnectionState::UnpreparedSend => {
                    self.test_cancel()?;
                    self.writer.send().await?;
                    self.state = ConnectionState::QueryReadHead;
                }
                ConnectionState::UnpreparedRecv => {
                    self.test_cancel()?;
                    let package = self.reader.read().await?;
                    let mut pp = PackageParser::new(package);
                    match pp.get_u8().loc("first_byte")? {
                        255 => {
                            self.state = ConnectionState::Broken;
                            handle_mysql_error(&mut pp)?;
                            unreachable!()
                        }
                        0 => {
                            self.state = ConnectionState::Clean;
                            return Ok(());
                        }
                        v => {
                            self.state = ConnectionState::Broken;
                            return Err(ConnectionErrorContent::ProtocolError(format!(
                                "Unexpected response type {v} to row package"
                            ))
                            .into());
                        }
                    }
                }
                ConnectionState::Broken => {
                    return Err(ConnectionErrorContent::ProtocolError(
                        "Previous protocol error reported".to_string(),
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    /// Prepare a statement.
    ///
    /// The cleanup functionally ensures that the future is drop drop safe
    async fn prepare_query(&mut self, stmt: &str) -> ConnectionResult<Statement> {
        assert!(matches!(self.state, ConnectionState::Clean));
        self.writer.seq = 0;
        let mut p = self.writer.compose();
        p.put_u8(com::STMT_PREPARE);
        p.put_bytes(stmt.as_bytes());
        p.finalize();

        self.state = ConnectionState::PrepareStatementSend;
        // safety-cancel: We set the state so that cleanup will get us in a good state
        self.test_cancel()?;
        self.writer.send().await?;

        self.state = ConnectionState::PrepareStatementReadHead;
        // safety-cancel: We set the state so that cleanup will get us in a good state
        self.test_cancel()?;
        let package = self.reader.read().await?;

        let mut p = PackageParser::new(package);
        match p.get_u8().loc("response type")? {
            0 => {
                let stmt_id = p.get_u32().loc("stmt_id")?;
                let num_columns = p.get_u16().loc("num_columns")?;
                let num_params = p.get_u16().loc("num_params")?;
                // We skip the rest of the package here

                // Read param definitions
                for p in 0..num_params {
                    self.state = ConnectionState::PrepareStatementReadParams {
                        params: num_params - p,
                        columns: num_columns,
                        stmt_id,
                    };
                    // safety-cancel: We set the state so that cleanup will get us in a good state
                    self.test_cancel()?;
                    self.reader.read().await?;
                    // We could use parse_column_definition if we care about the content
                }

                // Skip column definitions
                for c in 0..num_columns {
                    self.state = ConnectionState::PrepareStatementReadParams {
                        params: 0,
                        columns: num_columns - c,
                        stmt_id,
                    };
                    // safety-cancel: We set the state so that cleanup will get us in a good state
                    self.test_cancel()?;
                    self.reader.read().await?;
                    // We could use parse_column_definition if we care about the content
                }

                self.state = ConnectionState::Clean;
                Ok(Statement {
                    stmt_id,
                    num_params,
                })
            }
            255 => {
                handle_mysql_error(&mut p)?;
                unreachable!()
            }
            v => {
                self.state = ConnectionState::Broken;
                Err(ConnectionErrorContent::ProtocolError(format!(
                    "Unexpected response type {v} to prepare statement"
                ))
                .into())
            }
        }
    }

    /// Begin execution of a prepared statement
    fn query<'a>(&'a mut self, statement: &'a Statement) -> Query<'a> {
        assert!(matches!(self.state, ConnectionState::Clean));

        self.writer.seq = 0;
        let mut p = self.writer.compose();
        p.put_u8(com::STMT_EXECUTE);
        p.put_u32(statement.stmt_id);
        p.put_u8(0); // flags
        p.put_u32(1); // iteration_count

        let null_offset = p.writer.buff.len();
        let mut type_offset = null_offset;
        if statement.num_params != 0 {
            let null_bytes = statement.num_params.div_ceil(8);
            // Dummy null
            for _ in 0..null_bytes {
                p.put_u8(0);
            }
            p.put_u8(1); // Send types

            type_offset = p.writer.buff.len();
            // Dummy types and flags
            for _ in 0..statement.num_params {
                p.put_u16(0);
            }
        }

        Query {
            connection: self,
            statement,
            cur_param: 0,
            null_offset,
            type_offset,
        }
    }

    /// Execute a prepared statement
    async fn query_send(&mut self) -> ConnectionResult<QueryResult> {
        let p = Composer {
            writer: &mut self.writer,
        };
        p.finalize();

        self.state = ConnectionState::QuerySend;
        // safety-cancel: We have set the state so that clean can finish up
        self.test_cancel()?;
        self.writer.send().await?;

        self.state = ConnectionState::QueryReadHead;
        // safety-cancel: We have set the state so that clean can finish up
        self.test_cancel()?;
        let package = self.reader.read().await?;
        {
            let mut pp = PackageParser::new(package);
            match pp.get_u8().loc("first_byte")? {
                255 => {
                    handle_mysql_error(&mut pp)?;
                }
                0 => {
                    self.state = ConnectionState::Clean;
                    let affected_rows = pp.get_lenenc().loc("affected_rows")?;
                    let last_insert_id = pp.get_lenenc().loc("last_insert_id")?;
                    return Ok(QueryResult::ExecuteResult(ExecuteResult {
                        affected_rows,
                        last_insert_id,
                    }));
                }
                _ => (),
            }
        }

        let column_count = PackageParser::new(package)
            .get_lenenc()
            .loc("column_count")?;

        self.columns.clear();

        // Skip column definitions
        for c in 0..column_count {
            self.state = ConnectionState::QueryReadColumns(column_count - c);
            // safety-cancel: We have set the state so that clean can finish up
            self.test_cancel()?;
            let package = self.reader.read().await?;
            let mut p = PackageParser::new(package);
            self.columns.push(parse_column_definition(&mut p)?);
        }

        self.state = ConnectionState::QueryReadRows;
        Ok(QueryResult::WithColumns)
    }

    /// Execute an unprepared statement
    ///
    /// This is not implemented as a async method since we need to guarantee
    /// that the package has been composed correctly before the first await point
    fn execute_unprepared(
        &mut self,
        escaped_sql: &str,
    ) -> impl Future<Output = ConnectionResult<()>> + Send {
        assert!(matches!(self.state, ConnectionState::Clean));
        self.writer.seq = 0;
        let mut p = self.writer.compose();
        p.put_u8(com::QUERY);
        p.put_bytes(escaped_sql.as_bytes());
        p.finalize();

        self.state = ConnectionState::UnpreparedSend;

        async move {
            self.test_cancel()?;
            self.writer.send().await?;

            self.state = ConnectionState::UnpreparedRecv;
            self.test_cancel()?;
            let package = self.reader.read().await?;
            {
                let mut pp = PackageParser::new(package);
                match pp.get_u8().loc("first_byte")? {
                    255 => {
                        handle_mysql_error(&mut pp)?;
                    }
                    0 => {
                        self.state = ConnectionState::Clean;
                        return Ok(());
                    }
                    v => {
                        self.state = ConnectionState::Broken;
                        return Err(ConnectionErrorContent::ProtocolError(format!(
                            "Unexpected response type {v} to row package"
                        ))
                        .into());
                    }
                }
            }
            Ok(())
        }
    }
}

/// A connection to Mariadb/Mysql
pub struct Connection {
    /// Hash map of prepared statements.
    ///
    /// Note currently we do not clean up any prepared statements. In the future this will be turned into a LRU
    prepared_statements: HashMap<Cow<'static, str>, Statement>,
    /// Underlying raw connection
    raw: RawConnection,
    /// The current transaction depth
    transaction_depth: usize,
    /// The number of transactions to drop after cleanup
    cleanup_rollbacks: usize,
}

/// A query to Mariadb/Mysql
pub struct Query<'a> {
    /// The connection to make the query on
    connection: &'a mut RawConnection,
    /// The statement to execute
    statement: &'a Statement,
    /// The next param to bind
    cur_param: u16,
    /// Offset in the writer where the null array should be written
    null_offset: usize,
    /// Offset  in the writer where the types/unsigned should be written
    type_offset: usize,
}

impl<'a> Query<'a> {
    /// Bind the next argument to the query
    #[inline]
    pub fn bind<T: Bind>(mut self, v: &T) -> ConnectionResult<Self> {
        if self.cur_param == self.statement.num_params {
            return Err(ConnectionErrorContent::Bind(
                self.cur_param,
                BindError::TooManyArgumentsBound,
            )
            .into());
        }
        let mut w = crate::bind::Writer::new(&mut self.connection.writer.buff);
        if !v
            .bind(&mut w)
            .map_err(|e| ConnectionErrorContent::Bind(self.cur_param, e))?
        {
            let w = self.cur_param / 8;
            let b = self.cur_param % 8;
            self.connection.writer.buff[self.null_offset + w as usize] |= 1 << b;
        }

        self.connection.writer.buff[self.type_offset + (self.cur_param * 2) as usize] = T::TYPE;
        if T::UNSIGNED {
            self.connection.writer.buff[self.type_offset + (self.cur_param * 2) as usize + 1] = 128;
        }
        self.cur_param += 1;
        Ok(self)
    }

    /// Execute the query and return zero or one mapped rows
    ///
    /// All arguments must have been bound
    ///
    /// If the query returns more than one row an error is returned
    pub async fn fetch_optional_map<M: RowMap>(self) -> Result<Option<M::T<'a>>, M::E> {
        if self.cur_param != self.statement.num_params {
            return Err(ConnectionError::from(ConnectionErrorContent::Bind(
                self.cur_param,
                BindError::TooFewArgumentsBound,
            ))
            .into());
        }
        match self.connection.query_send().await? {
            QueryResult::WithColumns => (),
            QueryResult::ExecuteResult(_) => {
                return Err(ConnectionError::from(ConnectionErrorContent::ExpectedRows).into());
            }
        }

        // safety-cancel: The cleanup on the connection will skip the remaining rows
        self.connection.test_cancel()?;
        let p1 = self.connection.reader.read_raw().await?;
        {
            let mut pp = PackageParser::new(self.connection.reader.bytes(p1.clone()));
            match pp.get_u8().loc("Row first byte")? {
                0x00 => (),
                0xFE => {
                    //EOD
                    self.connection.state = ConnectionState::Clean;
                    return Ok(None);
                }
                0xFF => {
                    handle_mysql_error(&mut pp)?;
                    unreachable!()
                }
                v => {
                    return Err(ConnectionError::from(ConnectionErrorContent::ProtocolError(
                        format!("Unexpected response type {v} to row package"),
                    ))
                    .into());
                }
            }
        }

        // We need to keep two packages in memory, cleanup will unset this bool
        self.connection.reader.buffer_packages = true;

        // safety-cancel: The cleanup on the connection will skip the remaining rows
        self.connection.test_cancel()?;
        let p2 = self.connection.reader.read_raw().await?;
        {
            let mut pp = PackageParser::new(self.connection.reader.bytes(p2));
            match pp.get_u8().loc("Row first byte")? {
                0x00 => {
                    return Err(
                        ConnectionError::from(ConnectionErrorContent::UnexpectedRows).into(),
                    );
                }
                0xFE => {
                    self.connection.state = ConnectionState::Clean;
                }
                0xFF => {
                    handle_mysql_error(&mut pp)?;
                    unreachable!()
                }
                v => {
                    return Err(ConnectionError::from(ConnectionErrorContent::ProtocolError(
                        format!("Unexpected response type {v} to row package"),
                    ))
                    .into());
                }
            }
        }

        let row = Row::new(&self.connection.columns, self.connection.reader.bytes(p1));
        Ok(Some(M::map(row)?))
    }

    /// Execute the query and return zero or one rows
    ///
    /// All arguments must have been bound
    ///
    /// If the query returns more than one row an error is returned
    pub async fn fetch_optional<T: FromRow<'a>>(self) -> ConnectionResult<Option<T>> {
        if self.cur_param != self.statement.num_params {
            return Err(ConnectionErrorContent::Bind(
                self.cur_param,
                BindError::TooFewArgumentsBound,
            )
            .into());
        }
        match self.connection.query_send().await? {
            QueryResult::WithColumns => (),
            QueryResult::ExecuteResult(_) => {
                return Err(ConnectionErrorContent::ExpectedRows.into());
            }
        };

        // safety-cancel: The cleanup on the connection will skip the remaining rows
        self.connection.test_cancel()?;
        let p1 = self.connection.reader.read_raw().await?;
        {
            let mut pp = PackageParser::new(self.connection.reader.bytes(p1.clone()));
            match pp.get_u8().loc("Row first byte")? {
                0x00 => (),
                0xFE => {
                    //EOD
                    self.connection.state = ConnectionState::Clean;
                    return Ok(None);
                }
                0xFF => {
                    handle_mysql_error(&mut pp)?;
                    unreachable!()
                }
                v => {
                    return Err(ConnectionErrorContent::ProtocolError(format!(
                        "Unexpected response type {v} to row package"
                    ))
                    .into());
                }
            }
        }

        // We need to keep two packages in memory, cleanup will unset this bool
        self.connection.reader.buffer_packages = true;

        // safety-cancel: The cleanup on the connection will skip the remaining rows
        self.connection.test_cancel()?;
        let p2 = self.connection.reader.read_raw().await?;
        {
            let mut pp = PackageParser::new(self.connection.reader.bytes(p2));
            match pp.get_u8().loc("Row first byte")? {
                0x00 => return Err(ConnectionErrorContent::UnexpectedRows.into()),
                0xFE => {
                    self.connection.state = ConnectionState::Clean;
                }
                0xFF => {
                    handle_mysql_error(&mut pp)?;
                    unreachable!()
                }
                v => {
                    return Err(ConnectionErrorContent::ProtocolError(format!(
                        "Unexpected response type {v} to row package"
                    ))
                    .into());
                }
            }
        }

        let row = Row::new(&self.connection.columns, self.connection.reader.bytes(p1));
        Ok(Some(T::from_row(&row).loc("Row")?))
    }

    /// Execute the query and return one row
    ///
    /// All arguments must have been bound
    ///
    /// If the query does not return exactly one row an error is returned
    #[inline]
    pub async fn fetch_one<T: FromRow<'a>>(self) -> ConnectionResult<T> {
        match self.fetch_optional().await? {
            Some(v) => Ok(v),
            None => Err(ConnectionErrorContent::ExpectedRows.into()),
        }
    }

    /// Execute the query and return all mapped rows in a vector
    ///
    /// All arguments must have been bound
    pub async fn fetch_all_map<M: RowMap>(self) -> Result<Vec<M::T<'a>>, M::E> {
        if self.cur_param != self.statement.num_params {
            return Err(ConnectionError::from(ConnectionErrorContent::Bind(
                self.cur_param,
                BindError::TooFewArgumentsBound,
            ))
            .into());
        }
        match self.connection.query_send().await? {
            QueryResult::WithColumns => (),
            QueryResult::ExecuteResult(_) => {
                return Err(ConnectionError::from(ConnectionErrorContent::ExpectedRows).into());
            }
        };

        self.connection.ranges.clear();
        loop {
            // safety-cancel: The cleanup on the connection will skip the remaining rows
            self.connection.test_cancel()?;
            let p = self.connection.reader.read_raw().await?;
            {
                let mut pp = PackageParser::new(self.connection.reader.bytes(p.clone()));
                match pp.get_u8().loc("Row first byte")? {
                    0x00 => self.connection.ranges.push(p),
                    0xFE => {
                        //EOD
                        self.connection.state = ConnectionState::Clean;
                        break;
                    }
                    0xFF => {
                        handle_mysql_error(&mut pp)?;
                        unreachable!()
                    }
                    v => {
                        return Err(ConnectionError::from(ConnectionErrorContent::ProtocolError(
                            format!("Unexpected response type {v} to row package"),
                        ))
                        .into());
                    }
                }
            }

            // We need to keep two packages in memory, cleanup will unset this bool
            self.connection.reader.buffer_packages = true;
        }

        let mut ans = Vec::with_capacity(self.connection.ranges.len());
        for p in &self.connection.ranges {
            let row = Row::new(
                &self.connection.columns,
                self.connection.reader.bytes(p.clone()),
            );
            ans.push(M::map(row)?);
        }
        Ok(ans)
    }

    /// Execute the query and return all rows in a vector
    ///
    /// All arguments must have been bound
    pub async fn fetch_all<T: FromRow<'a>>(self) -> ConnectionResult<Vec<T>> {
        if self.cur_param != self.statement.num_params {
            return Err(ConnectionErrorContent::Bind(
                self.cur_param,
                BindError::TooFewArgumentsBound,
            )
            .into());
        }
        match self.connection.query_send().await? {
            QueryResult::WithColumns => (),
            QueryResult::ExecuteResult(_) => {
                return Err(ConnectionErrorContent::ExpectedRows.into());
            }
        };

        self.connection.ranges.clear();
        loop {
            // safety-cancel: The cleanup on the connection will skip the remaining rows
            self.connection.test_cancel()?;
            let p = self.connection.reader.read_raw().await?;
            {
                let mut pp = PackageParser::new(self.connection.reader.bytes(p.clone()));
                match pp.get_u8().loc("Row first byte")? {
                    0x00 => self.connection.ranges.push(p),
                    0xFE => {
                        //EOD
                        self.connection.state = ConnectionState::Clean;
                        break;
                    }
                    0xFF => {
                        handle_mysql_error(&mut pp)?;
                        unreachable!()
                    }
                    v => {
                        return Err(ConnectionErrorContent::ProtocolError(format!(
                            "Unexpected response type {v} to row package"
                        ))
                        .into());
                    }
                }
            }

            // We need to keep two packages in memory, cleanup will unset this bool
            self.connection.reader.buffer_packages = true;
        }

        let mut ans = Vec::with_capacity(self.connection.ranges.len());
        for p in &self.connection.ranges {
            let row = Row::new(
                &self.connection.columns,
                self.connection.reader.bytes(p.clone()),
            );
            ans.push(T::from_row(&row).loc("Row")?);
        }
        Ok(ans)
    }

    /// Execute the query and return an iterator that can return the results
    pub async fn fetch(self) -> ConnectionResult<QueryIterator<'a>> {
        if self.cur_param != self.statement.num_params {
            return Err(ConnectionErrorContent::Bind(
                self.cur_param,
                BindError::TooFewArgumentsBound,
            )
            .into());
        }
        match self.connection.query_send().await? {
            QueryResult::ExecuteResult(_) => Err(ConnectionErrorContent::ExpectedRows.into()),
            QueryResult::WithColumns => Ok(QueryIterator {
                connection: self.connection,
            }),
        }
    }

    /// Execute the query and return an iterator that can return the mapped results
    pub async fn fetch_map<M: RowMap>(self) -> ConnectionResult<MapQueryIterator<'a, M>> {
        if self.cur_param != self.statement.num_params {
            return Err(ConnectionErrorContent::Bind(
                self.cur_param,
                BindError::TooFewArgumentsBound,
            )
            .into());
        }
        match self.connection.query_send().await? {
            QueryResult::ExecuteResult(_) => Err(ConnectionErrorContent::ExpectedRows.into()),
            QueryResult::WithColumns => Ok(MapQueryIterator {
                connection: self.connection,
                _phantom: Default::default(),
            }),
        }
    }

    /// Execute query that does not return any columns
    pub async fn execute(self) -> ConnectionResult<ExecuteResult> {
        if self.cur_param != self.statement.num_params {
            return Err(ConnectionErrorContent::Bind(
                self.cur_param,
                BindError::TooFewArgumentsBound,
            )
            .into());
        }
        match self.connection.query_send().await? {
            QueryResult::WithColumns => Err(ConnectionErrorContent::UnexpectedRows.into()),
            QueryResult::ExecuteResult(v) => Ok(v),
        }
    }
}

/// Represents an ongoing transaction in the connection
///
/// Note: Since rust does not support async drops. Dropping
/// a transaction object will not roll back the transaction
/// immediately. This will instead be deferred to next time
/// the connection is used.
pub struct Transaction<'a> {
    /// The underlying connection we have started a transaction on
    connection: &'a mut Connection,
}

impl<'a> Transaction<'a> {
    /// Commit this traction to the database
    ///
    /// If the returned future is dropped. The transaction will
    /// be rolled back or committed the next time the underlying
    /// connection is used
    pub async fn commit(self) -> ConnectionResult<()> {
        let mut this = ManuallyDrop::new(self);
        this.connection.commit_impl().await?;
        Ok(())
    }

    /// Commit this traction to the database
    ///
    /// If the returned future is dropped. The transaction will
    /// be rolled back connection is used
    pub async fn rollback(self) -> ConnectionResult<()> {
        let mut this = ManuallyDrop::new(self);
        this.connection.rollback_impl().await?;
        Ok(())
    }
}

impl<'a> Executor for Transaction<'a> {
    #[inline]
    fn query_raw(
        &mut self,
        stmt: Cow<'static, str>,
    ) -> impl Future<Output = ConnectionResult<Query<'_>>> + Send {
        self.connection.query_inner(stmt)
    }

    #[inline]
    fn begin(&mut self) -> impl Future<Output = ConnectionResult<Transaction<'_>>> + Send {
        self.connection.begin_impl()
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        // Register in connection to drop transaction in cleanup
        self.connection.cleanup_rollbacks += 1;
    }
}

/// Trait implemented by [Connection] and [Transaction] that facilitates executing queries or creating new transactions
pub trait Executor: Sized {
    /// Execute a query on the connection
    ///
    /// If the returned feature is dropped, or if the returned [Query] or [QueryIterator] is dropped,
    /// the connection will be left in a unclean state. Where the query can be in a half finished stare.
    ///
    /// If this is the case [Connection::is_clean] will return false. A call to [Connection::cleanup] will finish up the
    /// query as quickly as possibly.
    ///
    /// If query is called while [Connection::is_clean] is false, query will call [Connection::cleanup] before executing the next
    /// query
    fn query_raw(
        &mut self,
        stmt: Cow<'static, str>,
    ) -> impl Future<Output = ConnectionResult<Query<'_>>> + Send;

    /// Begin a new transaction (or Save point)
    ///
    /// If the returned future is dropped either now transaction will have been created, or it will be
    /// dropped again [Connection::cleanup]
    fn begin(&mut self) -> impl Future<Output = ConnectionResult<Transaction<'_>>> + Send;
}

/// Add helper methods to Executor to facilitate common operations
pub trait ExecutorExt {
    /// Execute a query on the connection
    ///
    /// If the returned feature is dropped, or if the returned [Query] or [QueryIterator] is dropped,
    /// the connection will be left in a unclean state. Where the query can be in a half finished stare.
    ///
    /// If this is the case [Connection::is_clean] will return false. A call to [Connection::cleanup] will finish up the
    /// query as quickly as possibly.
    ///
    /// If query is called while [Connection::is_clean] is false, query will call [Connection::cleanup] before executing the next
    /// query
    fn query(
        &mut self,
        stmt: impl Into<Cow<'static, str>>,
    ) -> impl Future<Output = ConnectionResult<Query<'_>>> + Send;

    /// Execute a query on the connection with the given arguments
    ///
    /// This is a shortcut for
    /// ```ignore
    /// args.bind_args(query(stmt).await?)?;
    /// ```
    ///
    /// See [Executor::query] for cancel/drop safety
    fn query_with_args(
        &mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<Query<'_>>>;

    /// Execute a query with the given arguments and return all rows as a vector
    ///
    /// This is a shortcut for
    /// ```ignore
    /// query_with_args(stmt, args).await?.fetch_all().await?
    /// ```
    ///
    /// See [Executor::query] for cancel/drop safety
    fn fetch_all<'a, T: FromRow<'a> + Send>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<Vec<T>>> + Send;

    /// Execute a query with the given arguments and return all rows mapped to a vector vector
    ///
    /// This is a shortcut for
    /// ```ignore
    /// query_with_args(stmt, args).await?.fetch_all_map(map).await?
    /// ```
    ///
    /// See [Executor::query] for cancel/drop safety
    fn fetch_all_map<'a, M: RowMap>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = Result<Vec<M::T<'a>>, M::E>> + Send;

    /// Execute a query with the given arguments and return one row
    ///
    /// This is a shortcut for
    /// ```ignore
    /// query_with_args(stmt, args).await?.fetch_one().await?
    /// ```
    ///
    /// See [Executor::query] for cancel/drop safety
    fn fetch_one<'a, T: FromRow<'a> + Send>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<T>> + Send;

    /// Execute a query with the given arguments and return one mapped row
    ///
    /// This is a shortcut for
    /// ```ignore
    /// query_with_args(stmt, args).await?.fetch_one_map(map).await?
    /// ```
    ///
    /// See [Executor::query] for cancel/drop safety
    fn fetch_one_map<'a, M: RowMap>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = Result<M::T<'a>, M::E>> + Send;

    /// Execute a query with the given arguments are return an optional row
    ///
    /// This is a shortcut for
    /// ```ignore
    /// query_with_args(stmt, args).await?.fetch_optional().await?
    /// ```
    ///
    /// See [Executor::query] for cancel/drop safety
    fn fetch_optional<'a, T: FromRow<'a> + Send>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<Option<T>>> + Send;

    /// Execute a query with the given arguments are return an optional mapped row
    ///
    /// This is a shortcut for
    /// ```ignore
    /// query_with_args(stmt, args).await?.fetch_optional_map(map).await?
    /// ```
    ///
    /// See [Executor::query] for cancel/drop safety
    fn fetch_optional_map<'a, M: RowMap>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = Result<Option<M::T<'a>>, M::E>> + Send;

    /// Executing a query with the given arg
    ///
    /// This is a shortcut for
    /// ```ignore
    /// query_with_args(stmt, args).await?.execute().await?
    /// ```
    ///
    /// See [Executor::query] for cancel/drop safety
    fn execute(
        &mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<ExecuteResult>> + Send;

    /// Execute a query with the given arguments and stream the results back
    ///
    /// This is a shortcut for
    /// ```ignore
    /// query_with_args(stmt, args).await?.fetch().await?
    /// ```
    ///
    /// See [Executor::query] for cancel/drop safety
    fn fetch(
        &mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<QueryIterator<'_>>> + Send;

    /// Execute a query with the given arguments and stream the mapped results back
    ///
    /// This is a shortcut for
    /// ```ignore
    /// query_with_args(stmt, args).await?.fetch_map(map).await?
    /// ```
    ///
    /// See [Executor::query] for cancel/drop safety
    fn fetch_map<'a, M: RowMap>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<MapQueryIterator<'a, M>>> + Send;
}

/// Implement [ExecutorExt::query_with_args] without stmt as a generic
async fn query_with_args_impl<'a, E: Executor + Sized + Send>(
    e: &'a mut E,
    stmt: Cow<'static, str>,
    args: impl Args + Send,
) -> ConnectionResult<Query<'a>> {
    let q = e.query_raw(stmt).await?;
    args.bind_args(q)
}

/// Implement [ExecutorExt::fetch_all] without stmt as a generic
async fn fetch_all_impl<'a, E: Executor + Sized + Send, T: FromRow<'a>>(
    e: &'a mut E,
    stmt: Cow<'static, str>,
    args: impl Args + Send,
) -> ConnectionResult<Vec<T>> {
    let q = e.query(stmt).await?;
    let q = args.bind_args(q)?;
    q.fetch_all().await
}

/// Implement [ExecutorExt::fetch_all_map] without stmt as a generic
async fn fetch_all_map_impl<'a, E: Executor + Sized + Send, M: RowMap>(
    e: &'a mut E,
    stmt: Cow<'static, str>,
    args: impl Args + Send,
) -> Result<Vec<M::T<'a>>, M::E> {
    let q = e.query(stmt).await?;
    let q = args.bind_args(q)?;
    q.fetch_all_map::<M>().await
}

/// Implement [ExecutorExt::fetch_one] without stmt as a generic
async fn fetch_one_impl<'a, E: Executor + Sized + Send, T: FromRow<'a> + Send>(
    e: &'a mut E,
    stmt: Cow<'static, str>,
    args: impl Args + Send,
) -> ConnectionResult<T> {
    let q = e.query(stmt).await?;
    let q = args.bind_args(q)?;
    match q.fetch_optional().await? {
        Some(v) => Ok(v),
        None => Err(ConnectionErrorContent::ExpectedRows.into()),
    }
}

/// Implement [ExecutorExt::fetch_one_map] without stmt as a generic
async fn fetch_one_map_impl<'a, E: Executor + Sized + Send, M: RowMap>(
    e: &'a mut E,
    stmt: Cow<'static, str>,
    args: impl Args + Send,
) -> Result<M::T<'a>, M::E> {
    let q = e.query(stmt).await.map_err(M::E::from)?;
    let q = args.bind_args(q).map_err(M::E::from)?;
    match q.fetch_optional_map::<M>().await? {
        Some(v) => Ok(v),
        None => Err(ConnectionError::from(ConnectionErrorContent::ExpectedRows).into()),
    }
}

/// Implement [ExecutorExt::fetch_optional] without stmt as a generic
async fn fetch_optional_impl<'a, E: Executor + Sized + Send, T: FromRow<'a> + Send>(
    e: &'a mut E,
    stmt: Cow<'static, str>,
    args: impl Args + Send,
) -> ConnectionResult<Option<T>> {
    let q = e.query(stmt).await?;
    let q = args.bind_args(q)?;
    q.fetch_optional().await
}

/// Implement [ExecutorExt::fetch_optional_map] without stmt as a generic
async fn fetch_optional_map_impl<'a, E: Executor + Sized + Send, M: RowMap>(
    e: &'a mut E,
    stmt: Cow<'static, str>,
    args: impl Args + Send,
) -> Result<Option<M::T<'a>>, M::E> {
    let q = e.query(stmt).await?;
    let q = args.bind_args(q)?;
    q.fetch_optional_map::<M>().await
}

/// Implement [ExecutorExt::execute] without stmt as a generic
async fn execute_impl<E: Executor + Sized + Send>(
    e: &mut E,
    stmt: Cow<'static, str>,
    args: impl Args + Send,
) -> ConnectionResult<ExecuteResult> {
    let q = e.query_raw(stmt).await?;
    let q = args.bind_args(q)?;
    q.execute().await
}

/// Implement [ExecutorExt::fetch] without stmt as a generic
async fn fetch_impl<'a, E: Executor + Sized + Send>(
    e: &'a mut E,
    stmt: Cow<'static, str>,
    args: impl Args + Send,
) -> ConnectionResult<QueryIterator<'a>> {
    let q = e.query(stmt).await?;
    let q = args.bind_args(q)?;
    q.fetch().await
}

/// Implement [ExecutorExt::fetch_map] without stmt as a generic
async fn fetch_map_impl<'a, E: Executor + Sized + Send, M: RowMap>(
    e: &'a mut E,
    stmt: Cow<'static, str>,
    args: impl Args + Send,
) -> ConnectionResult<MapQueryIterator<'a, M>> {
    let q = e.query(stmt).await?;
    let q = args.bind_args(q)?;
    q.fetch_map::<M>().await
}

impl<E: Executor + Sized + Send> ExecutorExt for E {
    #[inline]
    fn query(
        &mut self,
        stmt: impl Into<Cow<'static, str>>,
    ) -> impl Future<Output = ConnectionResult<Query<'_>>> + Send {
        self.query_raw(stmt.into())
    }

    #[inline]
    fn query_with_args(
        &mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<Query<'_>>> {
        query_with_args_impl(self, stmt.into(), args)
    }

    #[inline]
    fn fetch_all<'a, T: FromRow<'a> + Send>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<Vec<T>>> + Send {
        fetch_all_impl(self, stmt.into(), args)
    }

    #[inline]
    fn fetch_all_map<'a, M: RowMap>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = Result<Vec<M::T<'a>>, M::E>> + Send {
        fetch_all_map_impl::<E, M>(self, stmt.into(), args)
    }

    #[inline]
    fn fetch_one<'a, T: FromRow<'a> + Send>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<T>> + Send {
        fetch_one_impl(self, stmt.into(), args)
    }

    #[inline]
    fn fetch_one_map<'a, M: RowMap>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = Result<M::T<'a>, M::E>> + Send {
        fetch_one_map_impl::<E, M>(self, stmt.into(), args)
    }

    #[inline]
    fn fetch_optional<'a, T: FromRow<'a> + Send>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<Option<T>>> + Send {
        fetch_optional_impl(self, stmt.into(), args)
    }

    #[inline]
    fn fetch_optional_map<'a, M: RowMap>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = Result<Option<M::T<'a>>, M::E>> + Send {
        fetch_optional_map_impl::<E, M>(self, stmt.into(), args)
    }

    #[inline]
    fn execute(
        &mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<ExecuteResult>> + Send {
        execute_impl(self, stmt.into(), args)
    }

    #[inline]
    fn fetch(
        &mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<QueryIterator<'_>>> + Send {
        fetch_impl(self, stmt.into(), args)
    }

    #[inline]
    fn fetch_map<'a, M: RowMap>(
        &'a mut self,
        stmt: impl Into<Cow<'static, str>>,
        args: impl Args + Send,
    ) -> impl Future<Output = ConnectionResult<MapQueryIterator<'a, M>>> + Send {
        fetch_map_impl::<E, M>(self, stmt.into(), args)
    }
}

impl Connection {
    /// Connect to Mariadb/Mysql
    pub async fn connect(options: &ConnectionOptions<'_>) -> ConnectionResult<Self> {
        let raw = RawConnection::connect(options).await?;
        Ok(Connection {
            raw,
            prepared_statements: Default::default(),
            transaction_depth: 0,
            cleanup_rollbacks: 0,
        })
    }

    /// Return false if there are partially execute queries in the connection
    pub fn is_clean(&self) -> bool {
        matches!(self.raw.state, ConnectionState::Clean);
        true
    }

    /// Finish up any partially execute queries as quickly as possible
    pub async fn cleanup(&mut self) -> ConnectionResult<()> {
        self.raw.cleanup().await?;

        assert!(self.cleanup_rollbacks <= self.transaction_depth);
        if self.cleanup_rollbacks != 0 {
            let statement = match self.prepared_statements.entry(rollback_transaction_query(
                self.transaction_depth - self.cleanup_rollbacks,
            )) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => {
                    let r = self.raw.prepare_query(e.key()).await?;
                    e.insert(r)
                }
            };

            // Once raw.query has been called we will have roled back to this transaction level once raw.cleanup succeed
            self.transaction_depth -= self.cleanup_rollbacks;
            self.cleanup_rollbacks = 0;
            let q = self.raw.query(statement);
            q.execute().await?;
        }
        Ok(())
    }

    /// Execute query. This inner method exists because [Self::query] is template on the stmt type
    /// but we would like only one instantiation.
    async fn query_inner(&mut self, stmt: Cow<'static, str>) -> ConnectionResult<Query<'_>> {
        self.cleanup().await?;
        let statement = match self.prepared_statements.entry(stmt) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let r = self.raw.prepare_query(e.key()).await?;
                e.insert(r)
            }
        };
        Ok(self.raw.query(statement))
    }

    /// Begin a new transaction or save-point
    async fn begin_impl(&mut self) -> ConnectionResult<Transaction<'_>> {
        self.cleanup().await?;

        assert_eq!(self.cleanup_rollbacks, 0); // cleanup_rollback will be 0 after cleanup

        // Once we call query the state will be such that once raw.cleanup has been called
        // there will be one more transaction level
        let q = begin_transaction_query(self.transaction_depth);
        self.transaction_depth += 1;
        self.cleanup_rollbacks = 1;
        self.raw.execute_unprepared(&q).await?;

        // The execute has now succeeded so there is no need to role back the transaction
        assert_eq!(self.cleanup_rollbacks, 1);
        self.cleanup_rollbacks = 0;
        Ok(Transaction { connection: self })
    }

    /// Rollback the top most transaction or save point
    async fn rollback_impl(&mut self) -> ConnectionResult<()> {
        self.cleanup().await?;
        assert_eq!(self.cleanup_rollbacks, 0);
        assert_ne!(self.transaction_depth, 0);
        self.transaction_depth -= 1;

        // Once we call query the state will be such that once raw.cleanup has been called
        // there will be one less transaction
        self.raw
            .execute_unprepared(&rollback_transaction_query(self.transaction_depth))
            .await?;

        Ok(())
    }

    /// Commit the top most transaction or save point
    async fn commit_impl(&mut self) -> ConnectionResult<()> {
        self.cleanup().await?;
        assert_eq!(self.cleanup_rollbacks, 0);
        assert_ne!(self.transaction_depth, 0);

        self.transaction_depth -= 1;

        // Once we call query the state will be such that once raw.cleanup has been called
        // there will be one less transaction
        self.raw
            .execute_unprepared(&commit_transaction_query(self.transaction_depth))
            .await?;

        Ok(())
    }

    #[cfg(feature = "cancel_testing")]
    #[doc(hidden)]
    /// Set the cancel counts for testing
    pub fn set_cancel_count(&mut self, cnt: Option<usize>) {
        self.raw.cancel_count = cnt;
    }
}

impl Executor for Connection {
    #[inline]
    fn query_raw(
        &mut self,
        stmt: Cow<'static, str>,
    ) -> impl Future<Output = ConnectionResult<Query<'_>>> + Send {
        self.query_inner(stmt)
    }

    #[inline]
    fn begin(&mut self) -> impl Future<Output = ConnectionResult<Transaction<'_>>> + Send {
        self.begin_impl()
    }
}
