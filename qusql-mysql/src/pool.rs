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
//!         ConnectionOptions{
//!             address: "127.0.0.1:3307".parse().unwrap(),
//!             user: "user".into(),
//!             password: "pw".into(),
//!             database: "test".into(),
//!             ..Default::default()
//!         },
//!         PoolOptions{
//!              max_connections: 10,
//!             ..Default::default()
//!         }
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
    time::Duration,
};

use crate::connection::{Connection, ConnectionOptions, ConnectionResult};

/// Options used for connection pool
pub struct PoolOptions {
    /// With this long when a connection is dropped while it is performing a query.
    ///
    /// After timeout the connection is closed. And a new connection may then be opened
    pub clean_timeout: Duration,
    /// Wait this long to attempt to connection again if we fail to connect
    pub reconnect_time: Duration,
    /// The maximum number of concurrent connections allowed
    pub max_connections: usize,
}

impl Default for PoolOptions {
    fn default() -> Self {
        Self {
            clean_timeout: Duration::from_millis(200),
            reconnect_time: Duration::from_secs(2),
            max_connections: 5,
        }
    }
}

/// Part of pool state protected by a mutex
struct PoolProtected {
    /// Current free transactions
    connections: Vec<Connection>,
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

/// Struct used to borrow a unallocated_connections counter from the pool
/// while the connection is being established
struct AddConnectionAvailableOnDrop(ManuallyDrop<Pool>);

impl AddConnectionAvailableOnDrop {
    /// Create a new AddConnectionAvailableOnDrop
    fn new(pool: Pool) -> Self {
        Self(ManuallyDrop::new(pool))
    }

    /// The connection has been establish successfully, release be and do not increment unallocated_connections
    fn take(mut self) -> Pool {
        // Safety: It is safe to take self.0 here since we explicitly forget ourselfs after
        let pool = unsafe { ManuallyDrop::take(&mut self.0) };
        std::mem::forget(self);
        pool
    }
}

impl Drop for AddConnectionAvailableOnDrop {
    fn drop(&mut self) {
        // Safety: We only take the value here and in take, which explicit called forget
        let pool = unsafe { ManuallyDrop::take(&mut self.0) };
        pool.connection_dropped();
    }
}

impl Pool {
    /// Establish a new pool with at least one connection
    pub async fn connect(
        connection_options: ConnectionOptions<'static>,
        pool_options: PoolOptions,
    ) -> ConnectionResult<Self> {
        let connection = Connection::connect(&connection_options).await?;
        Ok(Pool(Arc::new(PoolInner {
            protected: Mutex::new(PoolProtected {
                connections: vec![connection],
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
        loop {
            let token = {
                let mut inner = self.0.protected.lock().unwrap();
                if let Some(connection) = inner.connections.pop() {
                    return Ok(PoolConnection {
                        pool: self.clone(),
                        connection: ManuallyDrop::new(connection),
                    });
                }
                if inner.unallocated_connections == 0 {
                    None
                } else {
                    inner.unallocated_connections -= 1;
                    Some(AddConnectionAvailableOnDrop::new(self.clone()))
                }
            };
            if let Some(token) = token {
                // Safety cancel: This is cancel safe since the token will increment the unallocated_connections when dropped
                let r = Connection::connect(&self.0.connection_options).await;
                match r {
                    Ok(connection) => {
                        let pool = token.take();
                        return Ok(PoolConnection {
                            pool,
                            connection: ManuallyDrop::new(connection),
                        });
                    }
                    Err(e) => {
                        // Wait a bit with releasing the token, since the next acquire will probably run into the same failure
                        tokio::task::spawn(async move {
                            tokio::time::sleep(token.0.0.pool_options.reconnect_time).await;
                            std::mem::drop(token);
                        });
                        return Err(e);
                    }
                }
            } else {
                // Safety cancel: We are not holding any resources
                self.0.connection_available.notified().await
            }
        }
    }

    /// A connection has been dropped, allow new connections to be established
    fn connection_dropped(&self) {
        let mut inner = self.0.protected.lock().unwrap();
        if inner.connections.is_empty() && inner.unallocated_connections == 0 {
            self.0.connection_available.notify_one();
        }
        assert_ne!(inner.unallocated_connections, 0);
        inner.unallocated_connections -= 1;
    }

    /// Put a connection back into the pool
    fn release(&self, connection: Connection) {
        let mut inner = self.0.protected.lock().unwrap();
        if inner.connections.is_empty() && inner.unallocated_connections == 0 {
            self.0.connection_available.notify_one();
        }
        inner.connections.push(connection);
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
                        // Timeout durin cleaning
                        std::mem::drop(connection);
                        pool.connection_dropped();
                    }
                }
            });
        }
    }
}
