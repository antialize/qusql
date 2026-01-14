//! Implements a pool of connections to Mariadb/Mysql
//!
//! Example:
//! --------
//! ```no_run
//! use qusql_mysql::connection::{ConnectionOptions, ConnectionError, ExecutorExt};
//! use qusql_mysql::pool::{Pool, PoolOptions};
//!
//! async fn test() -> Result<(), ConnectionError> {
//!     let pool = Pool::connect(
//!         ConnectionOptions::new()
//!             .address("127.0.0.1:3307").unwrap()
//!             .user("user")
//!             .password("pw")
//!             .database("test"),
//!         PoolOptions::new().max_connections(10)
//!     ).await?;
//!
//!     let mut conn = pool.acquire().await?;
//!
//!     let row: Option<(i64,)> = conn.fetch_optional(
//!         "SELECT `number` FROM `table` WHERE `id`=?",
//!         (42,)
//!     ).await?;
//!
//!     if let Some((id,)) = row {
//!         println!("Found id {}", id);
//!     }
//!
//!     Ok(())
//! }
//! ```
use std::{
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    Executor,
    connection::{Connection, ConnectionOptions, ConnectionResult},
    handle_drop::HandleDrop,
};

/// Options used for connection pool
pub struct PoolOptions {
    /// With this long when a connection is dropped while it is performing a query.
    ///
    /// After timeout the connection is closed. And a new connection may then be opened
    clean_timeout: Duration,
    /// Wait this long to attempt to connection again if we fail to connect
    reconnect_time: Duration,
    /// The maximum number of concurrent connections allowed
    max_connections: usize,
    /// When acquiring a connection from the pool that is older than this, ping it first
    /// to ensure that it is still good
    stale_connection_time: Duration,
    /// When pinning a stale connection only wait this long
    ping_timeout: Duration,
}

impl PoolOptions {
    /// New default pool options
    pub fn new() -> Self {
        PoolOptions::default()
    }

    /// With this long when a connection is dropped while it is performing a query.
    ///
    /// After timeout the connection is closed. And a new connection may then be opened
    pub fn clean_timeout(self, duration: Duration) -> Self {
        PoolOptions {
            clean_timeout: duration,
            ..self
        }
    }

    /// Wait this long to attempt to connection again if we fail to connect
    pub fn reconnect_time(self, duration: Duration) -> Self {
        PoolOptions {
            reconnect_time: duration,
            ..self
        }
    }

    /// The maximum number of concurrent connections allowed
    pub fn max_connections(self, connection: usize) -> Self {
        PoolOptions {
            max_connections: connection,
            ..self
        }
    }
}

impl Default for PoolOptions {
    fn default() -> Self {
        Self {
            clean_timeout: Duration::from_millis(200),
            reconnect_time: Duration::from_secs(2),
            stale_connection_time: Duration::from_secs(10 * 60),
            ping_timeout: Duration::from_millis(200),
            max_connections: 5,
        }
    }
}

/// Part of pool state protected by a mutex
struct PoolProtected {
    /// Current free transactions
    connections: Vec<(Connection, Instant)>,
    /// Number of transactions we are still allowed to allocate
    unallocated_connections: usize,
}

/// Inner state of a pool
struct PoolInner {
    /// Part of state protected by a mutex
    protected: Mutex<PoolProtected>,
    /// The pool options given at creation time
    pool_options: PoolOptions,
    /// The connection options given at creation time
    connection_options: ConnectionOptions<'static>,
    /// Notify this when a connection becomes available
    connection_available: tokio::sync::Notify,
}

/// A pool of shared connections that can be acquired
#[derive(Clone)]
pub struct Pool(Arc<PoolInner>);

impl Pool {
    /// Establish a new pool with at least one connection
    pub async fn connect(
        connection_options: ConnectionOptions<'static>,
        pool_options: PoolOptions,
    ) -> ConnectionResult<Self> {
        let connection = Connection::connect(&connection_options).await?;
        Ok(Pool(Arc::new(PoolInner {
            protected: Mutex::new(PoolProtected {
                connections: vec![(connection, std::time::Instant::now())],
                unallocated_connections: pool_options.max_connections - 1,
            }),
            pool_options,
            connection_options,
            connection_available: tokio::sync::Notify::new(),
        })))
    }

    /// Acquire a free connection from the pool.
    ///
    /// If there is no free connection wait for one to become available
    ///
    /// The returned future is drop safe
    pub async fn acquire(&self) -> ConnectionResult<PoolConnection> {
        enum Res<N, R> {
            /// Wait for a connection to become available
            Wait,
            /// Establish a new connection
            New(N),
            /// Reuse an existing connection
            Reuse(R),
        }
        loop {
            let res = {
                let mut inner = self.0.protected.lock().unwrap();
                if let Some((connection, last_use)) = inner.connections.pop() {
                    Res::Reuse(HandleDrop::new(
                        (connection, last_use, self.clone()),
                        |(connection, last_use, pool)| {
                            let mut inner = pool.0.protected.lock().unwrap();
                            inner.connections.push((connection, last_use));
                        },
                    ))
                } else if inner.unallocated_connections == 0 {
                    Res::Wait
                } else {
                    inner.unallocated_connections -= 1;
                    Res::New(HandleDrop::new(self.clone(), |pool| {
                        pool.connection_dropped();
                    }))
                }
            };

            match res {
                Res::Wait => {
                    // Safety cancel: We are not holding any resources
                    self.0.connection_available.notified().await
                }
                Res::New(handle) => {
                    // Safety cancel: This is cancel safe since the handle will increment the unallocated_connections when dropped
                    let r = Connection::connect(&self.0.connection_options).await;
                    match r {
                        Ok(connection) => {
                            let pool = handle.release();
                            return Ok(PoolConnection {
                                pool,
                                connection: ManuallyDrop::new(connection),
                            });
                        }
                        Err(e) => {
                            // Wait a bit with releasing the handle, since the next acquire will probably run into the same failure
                            tokio::task::spawn(async move {
                                tokio::time::sleep((*handle).0.pool_options.reconnect_time).await;
                                std::mem::drop(handle);
                            });
                            return Err(e);
                        }
                    }
                }
                Res::Reuse(mut handle) => {
                    let (connection, last_use, pool) = &mut *handle;
                    if last_use.elapsed() > pool.0.pool_options.stale_connection_time {
                        // Safety cancel: This is cancel safe since the handle will put the connection back into the pool
                        match tokio::time::timeout(
                            pool.0.pool_options.ping_timeout,
                            connection.ping(),
                        )
                        .await
                        {
                            Ok(Ok(())) => (),
                            Err(_) | Ok(Err(_)) => {
                                // Ping failed or time outed. Lets drop the connection and create a new one
                                let (connection, _, pool) = handle.release();
                                std::mem::drop(connection);
                                pool.connection_dropped();
                                continue;
                            }
                        }
                    }
                    let (connection, _, pool) = handle.release();
                    let connection = PoolConnection {
                        pool,
                        connection: ManuallyDrop::new(connection),
                    };
                    return Ok(connection);
                }
            }
        }
    }

    /// A connection has been dropped, allow new connections to be established
    fn connection_dropped(&self) {
        let mut inner = self.0.protected.lock().unwrap();
        inner.unallocated_connections += 1;
        self.0.connection_available.notify_one();
    }

    /// Put a connection back into the pool
    fn release(&self, connection: Connection) {
        let mut inner = self.0.protected.lock().unwrap();
        self.0.connection_available.notify_one();
        inner
            .connections
            .push((connection, std::time::Instant::now()));
    }
}

/// A connection borrowed from the pool
pub struct PoolConnection {
    /// The pool the connection is borrowed from
    pool: Pool,
    /// The borrowed connection
    connection: ManuallyDrop<Connection>,
}

impl Deref for PoolConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl DerefMut for PoolConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl Drop for PoolConnection {
    /// Drop the connection, if we are in the middle of a request wait a bit for it to finish
    fn drop(&mut self) {
        // Safety: I will not access self.connection after this
        let mut connection = unsafe { ManuallyDrop::take(&mut self.connection) };
        if connection.is_clean() {
            self.pool.release(connection);
        } else {
            // The connection is not clean, lets try to clean it up for a bit
            let pool = self.pool.clone();
            tokio::spawn(async move {
                match tokio::time::timeout(pool.0.pool_options.clean_timeout, connection.cleanup())
                    .await
                {
                    Ok(Ok(())) => {
                        pool.release(connection);
                    }
                    Ok(Err(_)) => {
                        // Connection error during cleaning, lets just close the connection
                        std::mem::drop(connection);
                        pool.connection_dropped();
                    }
                    Err(_) => {
                        // Timeout during cleaning
                        std::mem::drop(connection);
                        pool.connection_dropped();
                    }
                }
            });
        }
    }
}
