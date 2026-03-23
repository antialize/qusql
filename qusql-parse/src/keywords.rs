//! SQL keyword definitions and related types.
//!
//! This crate provides the `Keyword` enum and `Restrict` type for SQL keyword handling.
//! Keywords with reserved status are marked with comments that are parsed by build.rs.

// Include the generated data and constants
mod keyword_gen {
    include!(concat!(env!("OUT_DIR"), "/keyword_gen.rs"));
}

/// A bitset representing the restrict sets a keyword belongs to.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Restrict(u16);

impl core::ops::BitOr for Restrict {
    type Output = Self;
    #[inline]
    fn bitor(self, rhs: Self) -> Self::Output {
        let mut result = self;
        result.0 |= rhs.0;
        result
    }
}

impl Restrict {
    /// Checks if this restrict set has any common bits with another restrict set,
    /// i.e. if there is any restrict set that is a subset of both.
    #[inline]
    fn check(self, rhs: Self) -> bool {
        (self.0 & rhs.0) != 0
    }

    /// A empty restrict set
    pub const EMPTY: Self = Restrict(0b0);

    /// A restrict set for MARIADB reserved keywords.
    pub const MARIADB: Self = Restrict(0b1);

    /// A restrict set for POSTGRES reserved keywords.
    pub const POSTGRES: Self = Restrict(0b10);

    /// A restrict set for SQLITE reserved keywords.
    pub const SQLITE: Self = Restrict(0b100);

    // Start restrict set list

    /// Restrict keywords: USING
    pub const USING: Self = Restrict(0b1000);

    /// Restrict keywords: SET
    pub const UPDATE_TABLE: Self = Restrict(0b10000);

    // End restrict set list
}

// The keyword list below, is read by build.rs to generate the keyword data and functions.
// Each keyword is marked with comments indicating if it is within the reserved keyword
// list for a particular database

// Start of keyword list
/// A SQL keyword.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
#[derive(Default)]
#[repr(usize)]
pub(crate) enum Keyword {
    ADD,                 // reserved: mariadb, sqlite
    ALL,                 // reserved: mariadb, postgres, sqlite
    ALTER,               // reserved: mariadb, sqlite
    ANALYZE,             // reserved: mariadb, postgres
    AND,                 // reserved: mariadb, postgres, sqlite
    ANY,                 // reserved: postgres
    AS,                  // reserved: mariadb, postgres, sqlite
    ASC,                 // reserved: mariadb, postgres
    ASENSITIVE,          // reserved: mariadb
    AUTHORIZATION,       // reserved: postgres
    BEFORE,              // reserved: mariadb
    BETWEEN,             // reserved: mariadb, sqlite
    BIGINT,              // reserved: mariadb
    BINARY,              // reserved: mariadb, postgres
    BLOB,                // reserved: mariadb
    BOTH,                // reserved: mariadb, postgres
    BY,                  // reserved: mariadb
    CALL,                // reserved: mariadb
    CASCADE,             // reserved: mariadb
    CASE,                // reserved: mariadb, postgres, sqlite
    CAST,                // reserved: postgres
    CHANGE,              // reserved: mariadb
    CHAR,                // reserved: mariadb; expr_ident
    CHARACTER,           // reserved: mariadb
    CHECK,               // reserved: mariadb, postgres, sqlite
    COLLATE,             // reserved: mariadb, postgres, sqlite
    COLLATION,           // reserved: postgres
    COLUMN,              // reserved: mariadb, postgres
    COMMENT,             // reserved: mariadb
    COMMIT,              // reserved: sqlite
    CONCURRENTLY,        // reserved: postgres
    CONDITION,           // reserved: mariadb
    CONSTRAINT,          // reserved: mariadb, postgres, sqlite
    CONTINUE,            // reserved: mariadb
    CONVERT,             // reserved: mariadb
    CREATE,              // reserved: mariadb, postgres, sqlite
    CROSS,               // reserved: mariadb, postgres
    CURRENT_CATALOG,     // reserved: postgres; expr_ident
    CURRENT_DATE,        // reserved: mariadb, postgres; expr_ident
    CURRENT_ROLE,        // reserved: mariadb, postgres; expr_ident
    CURRENT_TIME,        // reserved: mariadb, postgres; expr_ident
    CURRENT_TIMESTAMP,   // reserved: mariadb, postgres; expr_ident
    CURRENT_USER,        // reserved: mariadb, postgres; expr_ident
    CURSOR,              // reserved: mariadb
    DATABASES,           // reserved: mariadb
    DAY_HOUR,            // reserved: mariadb
    DAY_MICROSECOND,     // reserved: mariadb
    DAY_MINUTE,          // reserved: mariadb
    DAY_SECOND,          // reserved: mariadb
    DEC,                 // reserved: mariadb
    DECIMAL,             // reserved: mariadb
    DECLARE,             // reserved: mariadb
    DEFAULT,             // reserved: mariadb, postgres, sqlite
    DEFERRABLE,          // reserved: postgres
    DELAYED,             // reserved: mariadb
    DELETE,              // reserved: mariadb, sqlite
    DELETE_DOMAIN_ID,    // reserved: mariadb
    DESC,                // reserved: mariadb, postgres
    DESCRIBE,            // reserved: mariadb
    DETERMINISTIC,       // reserved: mariadb
    DISTINCT,            // reserved: mariadb, postgres, sqlite
    DISTINCTROW,         // reserved: mariadb
    DIV,                 // reserved: mariadb
    DO,                  // reserved: postgres
    DOUBLE,              // reserved: mariadb
    DO_DOMAIN_IDS,       // reserved: mariadb
    DROP,                // reserved: mariadb, sqlite
    DUAL,                // reserved: mariadb
    EACH,                // reserved: mariadb
    ELSE,                // reserved: mariadb, postgres, sqlite
    ELSEIF,              // reserved: mariadb
    ENCLOSED,            // reserved: mariadb
    END,                 // reserved: mariadb, postgres, sqlite
    ESCAPE,              // reserved: sqlite
    ESCAPED,             // reserved: mariadb
    EXCEPT,              // reserved: mariadb, postgres, sqlite
    EXISTS,              // reserved: mariadb, sqlite
    EXIT,                // reserved: mariadb
    EXPLAIN,             // reserved: mariadb
    FALSE,               // reserved: mariadb, postgres
    FETCH,               // reserved: mariadb, postgres
    FLOAT,               // reserved: mariadb
    FLOAT4,              // reserved: mariadb
    FLOAT8,              // reserved: mariadb
    FOR,                 // reserved: mariadb, postgres
    FORCE,               // reserved: mariadb
    FOREIGN,             // reserved: mariadb, postgres, sqlite
    FROM,                // reserved: mariadb, postgres, sqlite
    FULL,                // reserved: postgres
    FULLTEXT,            // reserved: mariadb
    GRANT,               // reserved: mariadb, postgres
    GROUP,               // reserved: mariadb, postgres, sqlite
    HAVING,              // reserved: mariadb, postgres, sqlite
    HIGH_PRIORITY,       // reserved: mariadb
    HOUR_MICROSECOND,    // reserved: mariadb
    HOUR_MINUTE,         // reserved: mariadb
    HOUR_SECOND,         // reserved: mariadb
    IF,                  // reserved: mariadb, sqlite; expr_ident
    IGNORE,              // reserved: mariadb
    IGNORE_DOMAIN_IDS,   // reserved: mariadb
    IN,                  // reserved: mariadb, postgres, sqlite
    INDEX,               // reserved: mariadb, sqlite
    INFILE,              // reserved: mariadb
    INITIALLY,           // reserved: postgres
    INNER,               // reserved: mariadb, postgres
    INOUT,               // reserved: mariadb
    INSENSITIVE,         // reserved: mariadb
    INSERT,              // reserved: mariadb, sqlite; expr_ident
    INT,                 // reserved: mariadb
    INT1,                // reserved: mariadb
    INT2,                // reserved: mariadb
    INT3,                // reserved: mariadb
    INT4,                // reserved: mariadb
    INT8,                // reserved: mariadb
    INTEGER,             // reserved: mariadb
    INTERSECT,           // reserved: mariadb, postgres, sqlite
    INTERVAL,            // reserved: mariadb
    INTO,                // reserved: mariadb, postgres, sqlite
    IS,                  // reserved: mariadb, postgres, sqlite
    ITERATE,             // reserved: mariadb
    JOIN,                // reserved: mariadb, postgres, sqlite
    KEY,                 // reserved: mariadb
    KEYS,                // reserved: mariadb
    KILL,                // reserved: mariadb
    LEADING,             // reserved: mariadb, postgres
    LEAVE,               // reserved: mariadb
    LEFT,                // reserved: mariadb, postgres
    LIKE,                // reserved: mariadb, postgres
    LIMIT,               // reserved: mariadb, postgres, sqlite
    LINEAR,              // reserved: mariadb
    LINES,               // reserved: mariadb
    LOAD,                // reserved: mariadb
    LOCALTIME,           // reserved: mariadb, postgres; expr_ident
    LOCALTIMESTAMP,      // reserved: mariadb, postgres; expr_ident
    LOCK,                // reserved: mariadb
    LONG,                // reserved: mariadb
    LONGBLOB,            // reserved: mariadb
    LONGTEXT,            // reserved: mariadb
    LOOP,                // reserved: mariadb
    LOW_PRIORITY,        // reserved: mariadb
    MATCH,               // reserved: mariadb
    MAXVALUE,            // reserved: mariadb
    MEDIUMBLOB,          // reserved: mariadb
    MEDIUMINT,           // reserved: mariadb
    MEDIUMTEXT,          // reserved: mariadb
    MIDDLEINT,           // reserved: mariadb
    MINUTE_MICROSECOND,  // reserved: mariadb
    MINUTE_SECOND,       // reserved: mariadb
    MOD,                 // reserved: mariadb; expr_ident
    MODIFIES,            // reserved: mariadb
    NATURAL,             // reserved: mariadb, postgres
    NOT,                 // reserved: mariadb, postgres, sqlite
    NOTHING,             // reserved: sqlite
    NO_WRITE_TO_BINLOG,  // reserved: mariadb
    NULL,                // reserved: mariadb, postgres, sqlite
    NUMERIC,             // reserved: mariadb
    OFFSET,              // reserved: mariadb, postgres
    ON,                  // reserved: mariadb, postgres, sqlite
    ONLY,                // reserved: postgres
    OPTIMIZE,            // reserved: mariadb
    OPTIONALLY,          // reserved: mariadb
    OR,                  // reserved: mariadb, postgres, sqlite
    ORDER,               // reserved: mariadb, postgres, sqlite
    OUT,                 // reserved: mariadb
    OUTER,               // reserved: mariadb, postgres
    OUTFILE,             // reserved: mariadb
    OVER,                // reserved: mariadb
    OVERLAPS,            // reserved: postgres
    PAGE_CHECKSUM,       // reserved: mariadb
    PARSE_VCOL_EXPR,     // reserved: mariadb
    PARTITION,           // reserved: mariadb
    PORTION,             // reserved: mariadb
    PRECISION,           // reserved: mariadb
    PRIMARY,             // reserved: mariadb, postgres, sqlite
    PROCEDURE,           // reserved: mariadb
    PURGE,               // reserved: mariadb
    RANGE,               // reserved: mariadb
    READ,                // reserved: mariadb
    READS,               // reserved: mariadb
    READ_WRITE,          // reserved: mariadb
    REAL,                // reserved: mariadb
    RECURSIVE,           // reserved: mariadb
    REFERENCES,          // reserved: mariadb, postgres, sqlite
    REF_SYSTEM_ID,       // reserved: mariadb
    REGEXP,              // reserved: mariadb
    RELEASE,             // reserved: mariadb
    RENAME,              // reserved: mariadb
    REPEAT,              // reserved: mariadb; expr_ident
    REPLACE,             // reserved: mariadb; expr_ident
    REQUIRE,             // reserved: mariadb
    RESIGNAL,            // reserved: mariadb
    RESTRICT,            // reserved: mariadb
    RETURN,              // reserved: mariadb
    RETURNING,           // reserved: mariadb, postgres, sqlite
    REVOKE,              // reserved: mariadb
    RIGHT,               // reserved: mariadb, postgres; expr_ident
    RLIKE,               // reserved: mariadb
    ROWS,                // reserved: mariadb
    SCHEMAS,             // reserved: mariadb
    SECOND_MICROSECOND,  // reserved: mariadb
    SELECT,              // reserved: mariadb, postgres, sqlite
    SENSITIVE,           // reserved: mariadb
    SEPARATOR,           // reserved: mariadb
    SESSION_USER,        // reserved: postgres; expr_ident
    SET,                 // reserved: mariadb, postgres, sqlite
    SHOW,                // reserved: mariadb
    SIGNAL,              // reserved: mariadb
    SMALLINT,            // reserved: mariadb
    SOME,                // reserved: postgres
    SPATIAL,             // reserved: mariadb
    SPECIFIC,            // reserved: mariadb
    SQL,                 // reserved: mariadb
    SQLEXCEPTION,        // reserved: mariadb
    SQLSTATE,            // reserved: mariadb
    SQLWARNING,          // reserved: mariadb
    SQL_BIG_RESULT,      // reserved: mariadb
    SQL_CALC_FOUND_ROWS, // reserved: mariadb
    SQL_SMALL_RESULT,    // reserved: mariadb
    SSL,                 // reserved: mariadb
    STARTING,            // reserved: mariadb
    STATS_AUTO_RECALC,   // reserved: mariadb
    STATS_PERSISTENT,    // reserved: mariadb
    STATS_SAMPLE_PAGES,  // reserved: mariadb
    STRAIGHT_JOIN,       // reserved: mariadb
    TABLE,               // reserved: mariadb, postgres, sqlite
    TERMINATED,          // reserved: mariadb
    THEN,                // reserved: mariadb, postgres, sqlite
    TINYBLOB,            // reserved: mariadb
    TINYINT,             // reserved: mariadb
    TINYTEXT,            // reserved: mariadb
    TO,                  // reserved: mariadb, postgres, sqlite
    TRAILING,            // reserved: mariadb, postgres
    TRANSACTION,         // reserved: sqlite
    TRIGGER,             // reserved: mariadb
    TRUE,                // reserved: mariadb, postgres
    UNDO,                // reserved: mariadb
    UNION,               // reserved: mariadb, postgres, sqlite
    UNIQUE,              // reserved: mariadb, postgres, sqlite
    UNLOCK,              // reserved: mariadb
    UNSIGNED,            // reserved: mariadb
    UPDATE,              // reserved: mariadb, sqlite
    USAGE,               // reserved: mariadb
    USE,                 // reserved: mariadb
    USER,                // reserved: postgres; expr_ident
    USING,               // reserved: mariadb, postgres, sqlite
    UTC_DATE,            // reserved: mariadb; expr_ident
    UTC_TIME,            // reserved: mariadb; expr_ident
    UTC_TIMESTAMP,       // reserved: mariadb; expr_ident
    VALUES,              // reserved: mariadb, sqlite; expr_ident
    VARBINARY,           // reserved: mariadb
    VARCHAR,             // reserved: mariadb
    VARCHARACTER,        // reserved: mariadb
    VARYING,             // reserved: mariadb
    WHEN,                // reserved: mariadb, postgres, sqlite
    WHERE,               // reserved: mariadb, postgres, sqlite
    WHILE,               // reserved: mariadb
    WINDOW,              // reserved: postgres
    WITH,                // reserved: mariadb, postgres
    WRITE,               // reserved: mariadb
    XOR,                 // reserved: mariadb
    YEAR_MONTH,          // reserved: mariadb
    ZEROFILL,            // reserved: mariadb
    #[default]
    NOT_A_KEYWORD,
    QUOTED_IDENTIFIER,
    // Possibly restricted keywords above. Keywords never restricted below.
    ABS,
    ACCESSIBLE,
    ACCOUNT,
    ACOS,
    ACTION,
    ADDDATE,
    ADDTIME,
    ADD_MONTHS,
    ADMIN,
    AFTER,
    AGAINST,
    AGGREGATE,
    ALGORITHM,
    ALWAYS,
    ARRAY,
    ARRAY_AGG,
    ASCII,
    ASIN,
    AT,
    ATAN,
    ATAN2,
    ATTRIBUTE,
    ATOMIC,
    AUTHORS,
    AUTO,
    AUTOEXTEND_SIZE,
    AUTO_INCREMENT,
    AVG,
    AVG_ROW_LENGTH,
    BACKUP,
    BEGIN,
    BIN,
    BINLOG,
    BIT,
    BIT_AND,
    BIT_LENGTH,
    BIT_OR,
    BIT_XOR,
    BLOCK,
    BLOOM,
    BODY,
    BOOL,
    BOOL_AND,
    BOOL_OR,
    BOOLEAN,
    BOX,
    BPCHAR,
    BPCHAR_PATTERN_OPS,
    BRIN,
    BTREE,
    BUFFERS,
    BYPASSRLS,
    BYTE,
    BYTEA,
    CACHE,
    CALLED,
    CASCADED,
    CATALOG_NAME,
    CEIL,
    CEILING,
    CHAIN,
    CHANGED,
    CHANNEL,
    CHARACTER_LENGTH,
    CHARSET,
    CHAR_LENGTH,
    CHECKPOINT,
    CHECKSUM,
    CHR,
    CIDR,
    CIPHER,
    CIRCLE,
    CLASS,
    CLASS_ORIGIN,
    CLIENT,
    CLOB,
    CLOSE,
    COALESCE,
    CODE,
    COLUMNS,
    COLUMN_ADD,
    COLUMN_CHECK,
    COLUMN_CREATE,
    COLUMN_DELETE,
    COLUMN_GET,
    COLUMN_NAME,
    COMMITTED,
    COMMUTATOR,
    COMPACT,
    COMPLETION,
    COMPRESSED,
    COMPRESSION,
    CONCAT,
    CONCAT_WS,
    CONCURRENT,
    CONFLICT,
    CONNECTION,
    CONSISTENT,
    CONSTRAINT_CATALOG,
    CONSTRAINT_NAME,
    CONSTRAINT_SCHEMA,
    CONTAINS,
    CONTEXT,
    CONTRIBUTORS,
    CONV,
    CONVERT_TZ,
    COPY,
    CORR,
    COS,
    COSTS,
    COT,
    COUNT,
    COVAR_POP,
    COVAR_SAMP,
    CPU,
    CRC32,
    CRC32C,
    CSV,
    CREATEDB,
    CREATEROLE,
    CUBE,
    CUME_DIST,
    CURDATE,
    CURRENT,
    CURRENT_POS,
    CURSOR_NAME,
    CURTIME,
    CYCLE,
    DATA,
    DATABASE,
    DATAFILE,
    DATE,
    DATEMULTIRANGE,
    DATERANGE,
    DATEDIFF,
    DATETIME,
    DATE_ADD,
    DATE_FORMAT,
    DATE_SUB,
    DAY,
    DAYNAME,
    DAYOFMONTH,
    DAYOFWEEK,
    DENSE_RANK,
    DAYOFYEAR,
    DEALLOCATE,
    DEFERRED,
    DEFINER,
    DEGREES,
    DELAY_KEY_WRITE,
    DELIMITER,
    DES_KEY_FILE,
    DIAGNOSTICS,
    DIRECTORY,
    DISABLE,
    DISCARD,
    DISK,
    DOMAIN,
    DUMPFILE,
    DUPLICATE,
    DYNAMIC,
    ELSIF,
    ELT,
    EMPTY,
    ENABLE,
    ENCODING,
    ENCRYPTED,
    ENCRYPTION,
    ENDS,
    ENGINE,
    ENGINES,
    ENGINE_ATTRIBUTE,
    ENUM,
    ERROR,
    ERRORS,
    EVENT,
    EVENTS,
    EVERY,
    EXAMINED,
    EXCEPTION,
    EXCHANGE,
    EXCLUDE,
    EXCLUSIVE,
    EXECUTE,
    EXP,
    EXPANSION,
    EXPIRE,
    EXPORT,
    EXPORT_SET,
    EXTENDED,
    EXTENSION,
    EXTENT_SIZE,
    EXTRACT,
    EXTRACTVALUE,
    FAMILY,
    FAST,
    FAULTS,
    FEDERATED,
    FIELD,
    FIELDS,
    FILE,
    FILTER,
    FIND_IN_SET,
    FIRST,
    FIXED,
    FLOOR,
    FLUSH,
    FOLLOWING,
    FOLLOWS,
    FORCE_NOT_NULL,
    FORCE_NULL,
    FORCE_QUOTE,
    FORMAT,
    FOUND,
    FREEZE,
    GENERIC_PLAN,
    FROM_BASE64,
    FROM_DAYS,
    FROM_UNIXTIME,
    FUNCTION,
    GENERAL,
    GENERATED,
    GENERATE_SERIES,
    GET,
    GET_FORMAT,
    GIN,
    GIST,
    GLOBAL,
    GOTO,
    GRANTS,
    GREATEST,
    GROUP_CONCAT,
    HANDLER,
    HARD,
    HEADER,
    HASH,
    HASHES,
    HELP,
    HEX,
    HISTORY,
    HNSW,
    HOLD,
    HOST,
    HOSTS,
    HOUR,
    ID,
    IDENTIFIED,
    IDENTITY,
    IFNULL,
    IGNORED,
    IGNORE_SERVER_IDS,
    IMMEDIATE,
    IMMUTABLE,
    IMPORT,
    INCLUDE,
    INCREMENT,
    INDEXES,
    INET,
    INET4,
    INET6,
    INHERIT,
    INHERITS,
    INITIAL_SIZE,
    INPLACE,
    INPUT,
    INSERT_METHOD,
    INSTALL,
    INSTANT,
    INSTEAD,
    INSTR,
    INT2_OPS,
    INT4_OPS,
    INT4MULTIRANGE,
    INT4RANGE,
    INT8_OPS,
    INT8MULTIRANGE,
    INT8RANGE,
    INTERSECTA,
    INVISIBLE,
    INVOKER,
    IO,
    IO_THREAD,
    IPC,
    ISOLATION,
    ISOPEN,
    ISSUER,
    JSON,
    JSONB,
    JSON_AGG,
    JSON_ARRAY,
    JSON_ARRAYAGG,
    JSON_ARRAY_APPEND,
    JSON_ARRAY_INSERT,
    JSON_ARRAY_INTERSECT,
    JSON_COMPACT,
    JSON_CONTAINS,
    JSON_CONTAINS_PATH,
    JSON_DEPTH,
    JSON_DETAILED,
    JSON_EQUALS,
    JSON_EXISTS,
    JSON_EXTRACT,
    JSON_INSERT,
    JSON_KEYS,
    JSON_LENGTH,
    JSON_LOOSE,
    JSON_MERGE,
    JSON_MERGE_PATCH,
    JSON_MERGE_PRESERVE,
    JSON_NORMALIZE,
    JSON_OBJECT,
    JSON_OBJECTAGG,
    JSONB_AGG,
    JSONB_OBJECT_AGG,
    JSON_OBJECT_FILTER_KEYS,
    JSON_OBJECT_TO_ARRAY,
    JSON_OVERLAPS,
    JSON_PRETTY,
    JSON_QUERY,
    JSON_QUOTE,
    JSON_REMOVE,
    JSON_REPLACE,
    JSON_SCHEMA_VALID,
    JSON_SEARCH,
    JSON_SET,
    JSON_TABLE,
    JSON_TYPE,
    JSON_UNQUOTE,
    JSON_VALID,
    JSON_VALUE,
    KEY_BLOCK_SIZE,
    LAG,
    LANGUAGE,
    LATERAL,
    LAST,
    LASTVAL,
    LAST_DAY,
    LAST_VALUE,
    LCASE,
    LEAD,
    LEAST,
    LEAVES,
    LEFTARG,
    LENGTH,
    LENGTHB,
    LESS,
    LEVEL,
    LINE,
    LSEG,
    LIST,
    LN,
    LOAD_FILE,
    LOCAL,
    LOCATE,
    LOCKED,
    LOCKS,
    LOG,
    LOG10,
    LOG2,
    LOGFILE,
    LOGIN,
    LOGS,
    LOG_VERBOSITY,
    LOWER,
    LPAD,
    LTRIM,
    MACADDR,
    MACADDR8,
    MAKEDATE,
    MAKETIME,
    MAKE_SET,
    MASTER,
    MASTER_CONNECT_RETRY,
    MASTER_DELAY,
    MASTER_GTID_POS,
    MASTER_HEARTBEAT_PERIOD,
    MASTER_HOST,
    MASTER_LOG_FILE,
    MASTER_LOG_POS,
    MASTER_PASSWORD,
    MASTER_PORT,
    MASTER_SERVER_ID,
    MASTER_SSL,
    MASTER_SSL_CA,
    MASTER_SSL_CAPATH,
    MASTER_SSL_CERT,
    MASTER_SSL_CIPHER,
    MASTER_SSL_CRL,
    MASTER_SSL_CRLPATH,
    MASTER_SSL_KEY,
    MASTER_SSL_VERIFY_SERVER_CERT,
    MASTER_USER,
    MASTER_USE_GTID,
    MATERIALIZED,
    MAX,
    MAX_CONNECTIONS_PER_HOUR,
    MAX_QUERIES_PER_HOUR,
    MAX_ROWS,
    MAX_SIZE,
    MAX_STATEMENT_TIME,
    MAX_UPDATES_PER_HOUR,
    MAX_USER_CONNECTIONS,
    MEDIUM,
    MEMBER,
    MEMORY,
    MERGE,
    MERGES,
    MESSAGE_TEXT,
    MICROSECOND,
    MID,
    MIGRATE,
    MIN,
    MINUS,
    MINUTE,
    MINVALUE,
    MIN_ROWS,
    MODE,
    MODIFY,
    MODULUS,
    MONEY,
    MONITOR,
    MONTH,
    MONTHNAME,
    MUTEX,
    MYSQL,
    MYSQL_ERRNO,
    NAME,
    NAMES,
    NATIONAL,
    NATURAL_SORT_KEY,
    NCHAR,
    NEGATOR,
    NESTED,
    NEVER,
    NEW,
    NEXT,
    NEXTVAL,
    NO,
    NOBYPASSRLS,
    NOCACHE,
    NOCREATEDB,
    NOCREATEROLE,
    NOCYCLE,
    NODEGROUP,
    NOINHERIT,
    NOLOGIN,
    NOMAXVALUE,
    NOMINVALUE,
    NONE,
    NOREPLICATION,
    NOSUPERUSER,
    NOTFOUND,
    NOTICE,
    NOW,
    NOWAIT,
    NO_WAIT,
    NULLIF,
    NULLS,
    NUMBER,
    NUMMULTIRANGE,
    NUMRANGE,
    NVARCHAR,
    NVL,
    NVL2,
    OCT,
    OCTET_LENGTH,
    OF,
    OFF,
    OLD,
    OLD_PASSWORD,
    ONE,
    ON_ERROR,
    ONLINE,
    OPEN,
    OPERATOR,
    OPTIMIZER,
    OPTIMIZER_COSTS,
    OPTION,
    OPTIONS,
    ORD,
    ORDINALITY,
    OTHERS,
    OWNED,
    OWNER,
    PACKAGE,
    PACK_KEYS,
    PAGE,
    PARALLEL,
    PARSER,
    PARTIAL,
    PARTITIONING,
    PARTITIONS,
    PASSWORD,
    PATH,
    PERIOD,
    PERCENT_RANK,
    PERCENTILE_CONT,
    PERCENTILE_DISC,
    PERIOD_ADD,
    PERIOD_DIFF,
    PERSISTENT,
    PHASE,
    PI,
    PLPGSQL,
    PLUGIN,
    PLUGINS,
    POINT,
    POLYGON,
    PORT,
    POSITION,
    POW,
    POWER,
    PRECEDES,
    PRECEDING,
    PREPARE,
    PRESERVE,
    PREV,
    PREVIOUS,
    PRIVILEGES,
    PROCESS,
    PROCESSLIST,
    PROGRAM,
    PROFILE,
    PROFILES,
    PROXY,
    QUARTER,
    QUERY,
    QUICK,
    QUOTE,
    RADIANS,
    RAISE,
    RAND,
    RANK,
    RAW,
    READ_ONLY,
    REBUILD,
    RECOVER,
    REDOFILE,
    REDO_BUFFER_SIZE,
    REDUNDANT,
    REJECT_LIMIT,
    RELAY,
    RELAYLOG,
    RELAY_LOG_FILE,
    RELAY_LOG_POS,
    RELAY_THREAD,
    RELOAD,
    REMAINDER,
    REMOVE,
    REORGANIZE,
    REPAIR,
    REPEATABLE,
    REPLAY,
    REFERENCING,
    REPLICA,
    REPLICAS,
    REPLICATION,
    REPLICA_POS,
    RESET,
    RESTART,
    RESTORE,
    RESUME,
    RETURNED_SQLSTATE,
    RETURNS,
    REUSE,
    REVERSE,
    RIGHTARG,
    ROLE,
    ROLLBACK,
    ROLLUP,
    ROUND,
    ROUTINE,
    ROW,
    REGR_AVGX,
    REGR_AVGY,
    REGR_COUNT,
    REGR_INTERCEPT,
    REGR_R2,
    REGR_SLOPE,
    REGR_SXX,
    REGR_SXY,
    REGR_SYY,
    ROWCOUNT,
    ROWNUM,
    ROWTYPE,
    ROWS_FROM,
    ROW_COUNT,
    ROW_FORMAT,
    RPAD,
    RTREE,
    RTRIM,
    RULE,
    SAVEPOINT,
    SCHEDULE,
    SCHEMA,
    SCHEMA_NAME,
    SCROLL,
    SEARCH,
    SECOND,
    SECONDARY,
    SECONDARY_ENGINE,
    SECONDARY_ENGINE_ATTRIBUTE,
    SECURITY,
    SEC_TO_TIME,
    SEQUENCE,
    SERIAL,
    SERIALIZABLE,
    SMALLSERIAL,
    BIGSERIAL,
    SERVER,
    SESSION,
    SETVAL,
    SFORMAT,
    SHARE,
    SHARED,
    SHUTDOWN,
    SIGN,
    SIGNED,
    SIMILAR,
    SIMPLE,
    SIN,
    SKIP,
    SLAVE,
    SLAVES,
    SLAVE_POS,
    SLEEP,
    SLOW,
    SNAPSHOT,
    SOCKET,
    SOFT,
    SONAME,
    SOUNDEX,
    SOUNDS,
    SOURCE,
    SPACE,
    SQL_BUFFER_RESULT,
    SQL_CACHE,
    SQL_NO_CACHE,
    SQL_THREAD,
    SQL_TSI_DAY,
    SQL_TSI_HOUR,
    SQL_TSI_MINUTE,
    SQL_TSI_MONTH,
    SQL_TSI_QUARTER,
    SQL_TSI_SECOND,
    SQL_TSI_WEEK,
    SQL_TSI_YEAR,
    SQRT,
    STABLE,
    SETTINGS,
    STAGE,
    START,
    STARTS,
    STARTS_WITH,
    STATEMENT,
    STATUS,
    STDIN,
    STDOUT,
    STOP,
    STORAGE,
    STORED,
    STD,
    STDDEV,
    STDDEV_POP,
    STDDEV_SAMP,
    STRCMP,
    STRFTIME,
    STRICT,
    STRING,
    STRING_AGG,
    STRING_TO_TABLE,
    STR_TO_DATE,
    SUBCLASS_ORIGIN,
    SUBDATE,
    SUBJECT,
    SUBPARTITION,
    SUBPARTITIONS,
    SUBSTR,
    SUBSTRING,
    SUBSTRING_INDEX,
    SUBTIME,
    SUM,
    SUPER,
    SUPERUSER,
    SUSPEND,
    SWAPS,
    SWITCHES,
    SYSDATE,
    SYSID,
    SYSTEM,
    SYSTEM_TIME,
    TABLES,
    TABLESPACE,
    TABLE_CHECKSUM,
    TABLE_NAME,
    TAN,
    TEMPORARY,
    TIMING,
    TEMPTABLE,
    TEXT,
    TEXT_PATTERN_OPS,
    THAN,
    THREADS,
    TIES,
    TIME,
    TIMEDIFF,
    TIMESTAMP,
    TIMESTAMPADD,
    TIMESTAMPDIFF,
    TIMESTAMPTZ,
    TIMETZ,
    TIME_FORMAT,
    TIME_TO_SEC,
    TIME_ZONE,
    TO_BASE64,
    TO_CHAR,
    TO_DAYS,
    TO_SECONDS,
    TRANSACTIONAL,
    TRIGGERS,
    TRIM, // expr_ident
    TRUNCATE,
    TSQUERY,
    TSMULTIRANGE,
    TSRANGE,
    TSTZMULTIRANGE,
    TSTZRANGE,
    TSVECTOR,
    TYPE,
    TYPES,
    UCASE,
    UNBOUNDED,
    UNCOMMITTED,
    SUMMARY,
    UNCOMPRESSED_LENGTH,
    UNDEFINED,
    UNDOFILE,
    UNDO_BUFFER_SIZE,
    UNHEX,
    UNICODE,
    UNINSTALL,
    UNIX_TIMESTAMP,
    REFRESH,
    UNKNOWN,
    UNNEST,
    UNTIL,
    UPDATEXML,
    UPGRADE,
    UPPER,
    UUID,
    USER_RESOURCES,
    USE_FRM,
    VALID,
    VALIDATE,
    VALUE,
    VAR_POP,
    VAR_SAMP,
    VARBIT,
    VARIANCE,
    VARCHAR2,
    VARCHAR_PATTERN_OPS,
    VARIABLES,
    VERSION,
    VERBOSE,
    VERSIONING,
    VIA,
    VIEW,
    VIRTUAL,
    VISIBLE,
    VOLATILE,
    WAIT,
    WAL,
    WARNINGS,
    WEEK,
    WEEKDAY,
    WEEKOFYEAR,
    WEIGHT_STRING,
    WITHIN,
    WITHOUT,
    WORK,
    WRAPPER,
    X509,
    XA,
    XMLAGG,
    XML,
    XMLTABLE,
    YAML,
    YEAR,
    YEARWEEK,
    ZONE,
    _LIST_,
    // MySQL 8.4 functions
    AES_DECRYPT,
    AES_ENCRYPT,
    ANY_VALUE,
    BENCHMARK,
    BIN_TO_UUID,
    BIT_COUNT,
    COERCIBILITY,
    COMPRESS,
    CONNECTION_ID,
    FIRST_VALUE,
    FORMAT_BYTES,
    FORMAT_PICO_TIME,
    FOUND_ROWS,
    GET_LOCK,
    GROUPING,
    ICU_VERSION,
    INET6_ATON,
    INET6_NTOA,
    INET_ATON,
    INET_NTOA,
    IS_FREE_LOCK,
    IS_IPV4,
    IS_IPV4_COMPAT,
    IS_IPV4_MAPPED,
    IS_IPV6,
    IS_USED_LOCK,
    IS_UUID,
    LAST_INSERT_ID,
    MD5,
    NAME_CONST,
    NTH_VALUE,
    NTILE,
    PS_CURRENT_THREAD_ID,
    PS_THREAD_ID,
    RANDOM_BYTES,
    REGEXP_INSTR,
    REGEXP_LIKE,
    REGEXP_REPLACE,
    REGEXP_SUBSTR,
    RELEASE_ALL_LOCKS,
    RELEASE_LOCK,
    ROLES_GRAPHML,
    ROW_NUMBER,
    SHA,
    SHA1,
    SHA2,
    STATEMENT_DIGEST,
    STATEMENT_DIGEST_TEXT,
    SYSTEM_USER,
    UNCOMPRESS,
    UUID_SHORT,
    UUID_TO_BIN,
    VALIDATE_PASSWORD_STRENGTH,
}
// End of keyword list

impl Keyword {
    /// Returns the name of this keyword as a string.
    pub fn name(&self) -> &'static str {
        keyword_gen::keyword_name(*self)
    }

    /// Checks if this keyword is reserved in any of the restrict sets.
    pub fn restricted(&self, restrict: Restrict) -> bool {
        keyword_gen::KEYWORDS_RESTRICT
            .get(*self as usize)
            .is_some_and(|r| r.check(restrict))
    }

    /// Checks if this keyword can be used as an identifier in an expression without quoting.
    pub const fn expr_ident(&self) -> bool {
        keyword_gen::keyword_expr_ident(*self)
    }
}

// Implement From<&str> for Keyword using the generated function
// if the string matches a known keyword, it returns that keyword, otherwise it returns NOT_A_KEYWORD.
impl From<&str> for Keyword {
    fn from(s: &str) -> Self {
        keyword_gen::keyword_from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_from_str() {
        assert_eq!(Keyword::from("SELECT"), Keyword::SELECT);
        assert_eq!(Keyword::from("select"), Keyword::SELECT);
        assert_eq!(Keyword::from("SeLeCt"), Keyword::SELECT);
        assert_eq!(Keyword::from("not_a_keyword"), Keyword::NOT_A_KEYWORD);
    }

    #[test]
    fn test_keyword_name() {
        assert_eq!(Keyword::SELECT.name(), "SELECT");
        assert_eq!(Keyword::CREATE.name(), "CREATE");
        assert_eq!(Keyword::NOT_A_KEYWORD.name(), "NOT_A_KEYWORD");
    }

    #[test]
    fn test_restrict_check() {
        // Test that reserved keywords work correctly
        assert!(Keyword::SELECT.restricted(Restrict::MARIADB));
        assert!(Keyword::SELECT.restricted(Restrict::POSTGRES));
    }
}
