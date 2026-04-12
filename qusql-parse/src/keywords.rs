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
    _LIST_,
    ABS,
    ACCESSIBLE,
    ACCOUNT,
    ACOS,
    ACOSD,
    ACOSH,
    ACTION,
    ADD_MONTHS,
    ADDDATE,
    ADDTIME,
    ADMIN,
    AES_DECRYPT,
    AES_ENCRYPT,
    AFTER,
    AGAINST,
    AGGREGATE,
    ALGORITHM,
    ALWAYS,
    ANY_VALUE,
    ARRAY_AGG,
    ARRAY,
    ARRAY_TO_TSVECTOR,
    AREA,
    ABBREV,
    ASCII,
    ASIN,
    ASIND,
    ASINH,
    AT,
    ATAN,
    ATAN2,
    ATAN2D,
    ATAND,
    ATANH,
    ATOMIC,
    ATTRIBUTE,
    AUTHORS,
    AUTO_INCREMENT,
    AUTO,
    AUTOEXTEND_SIZE,
    AVG_ROW_LENGTH,
    AVG,
    BACKUP,
    BEGIN,
    BENCHMARK,
    BIGSERIAL,
    BIN_TO_UUID,
    BIN,
    BINLOG,
    BIT_AND,
    BIT_COUNT,
    BIT_LENGTH,
    BIT_OR,
    BIT_XOR,
    BIT,
    BLOCK,
    BLOOM,
    BODY,
    BOOL_AND,
    BOOL_OR,
    BOOL,
    BOOLEAN,
    BOUND_BOX,
    BROADCAST,
    BOX,
    BOX2D,
    BOX3D,
    BPCHAR_PATTERN_OPS,
    BPCHAR,
    BRIN,
    BTREE,
    BTRIM,
    BUFFERS,
    BYPASSRLS,
    BYTE,
    BYTEA,
    CBRT,
    CACHE,
    CALLED,
    CASCADED,
    CASEFOLD,
    CATALOG_NAME,
    CEIL,
    CEILING,
    CENTER,
    CHAIN,
    CHANGED,
    CHANNEL,
    CHAR_LENGTH,
    CHARACTER_LENGTH,
    CHARSET,
    CHECKPOINT,
    CHECKSUM,
    CHR,
    CIDR,
    CIPHER,
    CIRCLE,
    CLASS_ORIGIN,
    CLASS,
    CLIENT,
    CLOB,
    CLOSE,
    COALESCE,
    CODE,
    COERCIBILITY,
    COLUMN_ADD,
    COLUMN_CHECK,
    COLUMN_CREATE,
    COLUMN_DELETE,
    COLUMN_GET,
    COLUMN_NAME,
    COLUMNS,
    COMMITTED,
    COMMUTATOR,
    COMPACT,
    COMPLETION,
    COMPRESS,
    COMPRESSED,
    COMPRESSION,
    CONCAT_WS,
    CONCAT,
    CONCURRENT,
    CONFLICT,
    CONNECT,
    CONNECTION_ID,
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
    COSD,
    COSH,
    COSTS,
    COT,
    COTD,
    COUNT,
    COVAR_POP,
    COVAR_SAMP,
    CPU,
    CRC32,
    CRC32C,
    CREATEDB,
    CREATEROLE,
    CSV,
    CUBE,
    CUME_DIST,
    CURDATE,
    CURRENT_POS,
    CURRENT,
    CURSOR_NAME,
    CURSOR_TO_XML,
    CURSOR_TO_XMLSCHEMA,
    CURTIME,
    CYCLE,
    DATA,
    DATABASE,
    DATABASE_TO_XML,
    DATABASE_TO_XML_AND_XMLSCHEMA,
    DATABASE_TO_XMLSCHEMA,
    DATAFILE,
    DATATYPE,
    DATE_ADD,
    DATE_FORMAT,
    DATE_SUB,
    DATE,
    DATEDIFF,
    DATEMULTIRANGE,
    DATERANGE,
    DATETIME,
    DAY,
    DAYNAME,
    DAYOFMONTH,
    DAYOFWEEK,
    DAYOFYEAR,
    DEALLOCATE,
    DEBUG,
    DEFERRED,
    DEFINER,
    DEGREES,
    DETAIL,
    DELAY_KEY_WRITE,
    DELIMITER,
    DENSE_RANK,
    DES_KEY_FILE,
    DIAGONAL,
    DIAMETER,
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
    ENGINE_ATTRIBUTE,
    ENGINE,
    ENGINES,
    ENUM,
    ENUM_FIRST,
    ENUM_LAST,
    ENUM_RANGE,
    ERF,
    ERFC,
    ERRCODE,
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
    EXPORT_SET,
    EXPORT,
    EXTENDED,
    EXTENSION,
    EXTENT_SIZE,
    EXTRACT,
    EXTRACTVALUE,
    FACTORIAL,
    FAMILY,
    FAST,
    FAULTS,
    FEDERATED,
    FIELD,
    FIELDS,
    FILE,
    FILTER,
    FIND_IN_SET,
    FIRST_VALUE,
    FIRST,
    FIXED,
    FLOOR,
    FLUSH,
    FOLLOWING,
    FOLLOWS,
    FORCE_NOT_NULL,
    FORCE_NULL,
    FORCE_QUOTE,
    FORMAT_BYTES,
    FORMAT_PICO_TIME,
    FORMAT,
    FOUND_ROWS,
    FOUND,
    FREEZE,
    FROM_BASE64,
    FROM_DAYS,
    FROM_UNIXTIME,
    FUNCTION,
    FUNCTIONS,
    GENERAL,
    GEN_RANDOM_UUID,
    GENERATE_SERIES,
    GENERATED,
    GENERIC_PLAN,
    GEOMETRYTYPE,
    GET_BIT,
    GET_CURRENT_TS_CONFIG,
    GET_FORMAT,
    GET_LOCK,
    GET,
    GAMMA,
    GCD,
    GIN,
    GIST,
    GLOBAL,
    GOTO,
    GRANTED,
    GRANTS,
    GREATEST,
    GROUP_CONCAT,
    GROUPING,
    HANDLER,
    HARD,
    HASH,
    HINT,
    HASHES,
    HEADER,
    HEIGHT,
    HELP,
    HEX,
    HISTORY,
    HNSW,
    HOLD,
    HOST,
    HOSTMASK,
    HOSTS,
    HOUR,
    ICU_VERSION,
    ID,
    IDENTIFIED,
    IDENTITY,
    IFNULL,
    IGNORE_SERVER_IDS,
    IGNORED,
    IMMEDIATE,
    IMMUTABLE,
    IMPORT,
    INCLUDE,
    INCREMENT,
    INDEXES,
    INET_ATON,
    INET_NTOA,
    INET,
    INET4,
    INET6_ATON,
    INET6_NTOA,
    INET6,
    INET_SERVER_ADDR,
    INET_SERVER_PORT,
    INET_MERGE,
    INET_SAME_FAMILY,
    INFO,
    INHERIT,
    INHERITS,
    INITIAL_SIZE,
    INPLACE,
    INPUT,
    INSERT_METHOD,
    INSTALL,
    INITCAP,
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
    IO_THREAD,
    IO,
    IPC,
    IS_FREE_LOCK,
    IS_IPV4_COMPAT,
    IS_IPV4_MAPPED,
    IS_IPV4,
    IS_IPV6,
    IS_USED_LOCK,
    IS_UUID,
    ISOLATION,
    ISCLOSED,
    ISOPEN,
    ISSUER,
    JSON_AGG,
    JSON_ARRAY_APPEND,
    JSON_ARRAY_INSERT,
    JSON_ARRAY_INTERSECT,
    JSON_ARRAY,
    JSON_ARRAYAGG,
    JSON_BUILD_OBJECT,
    JSON_COMPACT,
    JSON_CONTAINS_PATH,
    JSON_CONTAINS,
    JSON_DEPTH,
    JSON_DETAILED,
    JSON_EQUALS,
    JSON_EXISTS,
    JSON_EXTRACT,
    JSON_INSERT,
    JSON_KEYS,
    JSON_LENGTH,
    JSON_LOOSE,
    JSON_MERGE_PATCH,
    JSON_MERGE_PRESERVE,
    JSON_MERGE,
    JSON_NORMALIZE,
    JSON_OBJECT_FILTER_KEYS,
    JSON_OBJECT_TO_ARRAY,
    JSON_OBJECT,
    JSON_OBJECTAGG,
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
    JSON_TO_TSVECTOR,
    JSON_TYPE,
    JSON_UNQUOTE,
    JSON_VALID,
    JSON_VALUE,
    JSON,
    JSONB_AGG,
    JSONB_OBJECT_AGG,
    JSONB_SET,
    JSONB_TO_TSVECTOR,
    JSONB,
    KEY_BLOCK_SIZE,
    LAG,
    LANGUAGE,
    LARGE,
    LAST_DAY,
    LAST_INSERT_ID,
    LAST_VALUE,
    LAST,
    LASTVAL,
    LATERAL,
    LCASE,
    LCM,
    LEAD,
    LEAST,
    LEAVES,
    LEFTARG,
    LENGTH,
    LENGTHB,
    LESS,
    LEVEL,
    LGAMMA,
    LINE,
    LIST,
    LN,
    LOAD_FILE,
    LOCAL,
    LOCATE,
    LOCKED,
    LOCKS,
    LOG_VERBOSITY,
    LOG,
    LOG10,
    LOG2,
    LOGFILE,
    LOGIN,
    LOGS,
    LOWER,
    LPAD,
    LSEG,
    LTRIM,
    MACADDR,
    MACADDR8,
    MACADDR8_SET7BIT,
    MAINTAIN,
    MAKE_SET,
    MAKEDATE,
    MAKETIME,
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
    MASTER_SSL_CA,
    MASTER_SSL_CAPATH,
    MASTER_SSL_CERT,
    MASTER_SSL_CIPHER,
    MASTER_SSL_CRL,
    MASTER_SSL_CRLPATH,
    MASTER_SSL_KEY,
    MASTER_SSL_VERIFY_SERVER_CERT,
    MASTER_SSL,
    MASTER_USE_GTID,
    MASTER_USER,
    MASTER,
    MATERIALIZED,
    MAX_CONNECTIONS_PER_HOUR,
    MAX_QUERIES_PER_HOUR,
    MAX_ROWS,
    MAX_SIZE,
    MAX_STATEMENT_TIME,
    MAX_UPDATES_PER_HOUR,
    MAX_USER_CONNECTIONS,
    MAX,
    MD5,
    MEDIUM,
    MEMBER,
    MEMORY,
    MERGE,
    MERGES,
    MESSAGE,
    MESSAGE_TEXT,
    MICROSECOND,
    MID,
    MIGRATE,
    MIN_ROWS,
    MIN_SCALE,
    MIN,
    MINUS,
    MINUTE,
    MINVALUE,
    MODE,
    MODIFY,
    MODULUS,
    MONEY,
    MONITOR,
    MONTH,
    MONTHNAME,
    MUTEX,
    MYSQL_ERRNO,
    MYSQL,
    NAME_CONST,
    NAME,
    NAMES,
    NATIONAL,
    NATURAL_SORT_KEY,
    MASKLEN,
    NCHAR,
    NEGATOR,
    NESTED,
    NETMASK,
    NETWORK,
    NEVER,
    NEW,
    NEXT,
    NEXTVAL,
    NO_WAIT,
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
    NORMALIZE,
    NOSUPERUSER,
    NOTFOUND,
    NOTICE,
    NOW,
    NOWAIT,
    NPOINTS,
    NTH_VALUE,
    NTILE,
    NULLIF,
    NULLS,
    NUMBER,
    NUMMULTIRANGE,
    NUMNODE,
    NUMRANGE,
    NVARCHAR,
    NVL,
    NVL2,
    OBJECT,
    OCT,
    OCTET_LENGTH,
    OF,
    OFF,
    OLD_PASSWORD,
    OLD,
    ON_ERROR,
    ONE,
    ONLINE,
    OPEN,
    OPERATOR,
    OPTIMIZER_COSTS,
    OPTIMIZER,
    OPTION,
    OPTIONS,
    ORD,
    ORDINALITY,
    OTHERS,
    OWNED,
    OWNER,
    PACK_KEYS,
    PACKAGE,
    PAGE,
    PARALLEL,
    PARAMETER,
    PARSE_IDENT,
    PARSER,
    PARTIAL,
    PARTITIONING,
    PARTITIONS,
    PASSWORD,
    PATH,
    PCLOSE,
    POPEN,
    PERCENT_RANK,
    PERCENTILE_CONT,
    PERCENTILE_DISC,
    PERIOD_ADD,
    PERIOD_DIFF,
    PERIOD,
    PERFORM,
    PERSISTENT,
    PG_CLIENT_ENCODING,
    PG_POSTMASTER_START_TIME,
    PHASE,
    PI,
    PHRASETO_TSQUERY,
    PLAINTO_TSQUERY,
    PLPGSQL,
    PLUGIN,
    PLUGINS,
    POINT,
    POLYGON,
    PORT,
    POSITION,
    POSTGIS_FULL_VERSION,
    POW,
    POWER,
    PRECEDES,
    PRECEDING,
    PREPARE,
    PRESERVE,
    PREV,
    PREVIOUS,
    PRIVILEGES,
    PROCEDURES,
    PROCESS,
    PROCESSLIST,
    PROFILE,
    PROFILES,
    PROGRAM,
    PROXY,
    PS_CURRENT_THREAD_ID,
    PS_THREAD_ID,
    PUBLIC,
    QUARTER,
    QUERY,
    QUERYTREE,
    QUERY_TO_XML,
    QUERY_TO_XML_AND_XMLSCHEMA,
    QUERY_TO_XMLSCHEMA,
    QUICK,
    QUOTE,
    QUOTE_IDENT,
    QUOTE_LITERAL,
    QUOTE_NULLABLE,
    RADIANS,
    RADIUS,
    RAISE,
    RAND,
    RANDOM_BYTES,
    RANDOM_NORMAL,
    RANK,
    RAW,
    READ_ONLY,
    REBUILD,
    RECOVER,
    REDO_BUFFER_SIZE,
    REDOFILE,
    REDUNDANT,
    REFERENCING,
    REFRESH,
    REGEXP_COUNT,
    REGEXP_INSTR,
    REGEXP_LIKE,
    REGEXP_MATCH,
    REGEXP_MATCHES,
    REGEXP_REPLACE,
    REGEXP_SPLIT_TO_ARRAY,
    REGEXP_SPLIT_TO_TABLE,
    REGEXP_SUBSTR,
    REGR_AVGX,
    REGR_AVGY,
    REGR_COUNT,
    REGR_INTERCEPT,
    REGR_R2,
    REGR_SLOPE,
    REGR_SXX,
    REGR_SXY,
    REGR_SYY,
    REJECT_LIMIT,
    RELAY_LOG_FILE,
    RELAY_LOG_POS,
    RELAY_THREAD,
    RELAY,
    RELAYLOG,
    RELEASE_ALL_LOCKS,
    RELEASE_LOCK,
    RELEASE,
    RELOAD,
    REMAINDER,
    REMOVE,
    REORGANIZE,
    REPAIR,
    REPEATABLE,
    REPLAY,
    REPLICA_POS,
    REPLICA,
    REPLICAS,
    REPLICATION,
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
    ROLES_GRAPHML,
    ROLLBACK,
    ROLLUP,
    ROUND,
    ROUTINE,
    ROUTINES,
    ROW_COUNT,
    ROW_FORMAT,
    ROW_NUMBER,
    ROW,
    ROWCOUNT,
    ROWNUM,
    ROWS_FROM,
    ROWTYPE,
    RPAD,
    RTREE,
    RTRIM,
    RULE,
    SAVEPOINT,
    SCALE,
    SCHEDULE,
    SCHEMA_NAME,
    SCHEMA,
    SCHEMA_TO_XML,
    SCHEMA_TO_XML_AND_XMLSCHEMA,
    SCHEMA_TO_XMLSCHEMA,
    SCROLL,
    SEARCH,
    SEC_TO_TIME,
    SECOND,
    SECONDARY_ENGINE_ATTRIBUTE,
    SECONDARY_ENGINE,
    SECONDARY,
    SECURITY,
    SEQUENCE,
    SEQUENCES,
    SERIAL,
    SERIALIZABLE,
    SERVER,
    SESSION,
    SETSEED,
    SETTINGS,
    SETVAL,
    SET_BIT,
    SET_MASKLEN,
    SETWEIGHT,
    SFORMAT,
    SHA,
    SHA1,
    SHA2,
    SHARE,
    SHARED,
    SHUTDOWN,
    SIGN,
    SIGNED,
    SIMILAR,
    SIMPLE,
    SIN,
    SIND,
    SINH,
    SKIP,
    SLAVE_POS,
    SLAVE,
    SLAVES,
    SLEEP,
    SLOPE,
    SLOW,
    SMALLSERIAL,
    SNAPSHOT,
    SOCKET,
    SOFT,
    SONAME,
    SOUNDEX,
    SOUNDS,
    SOURCE,
    SPACE,
    SPLIT_PART,
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
    STAGE,
    START,
    STARTS_WITH,
    STARTS,
    STATEMENT_DIGEST_TEXT,
    STATEMENT_DIGEST,
    STATEMENT,
    STATUS,
    STD,
    STDDEV_POP,
    STDDEV_SAMP,
    STDDEV,
    STDIN,
    STDOUT,
    STOP,
    STORAGE,
    STORED,
    ST_ADDMEASURE,
    ST_ADDPOINT,
    ST_AFFINE,
    ST_AREA,
    ST_ASBINARY,
    ST_ASEWKB,
    ST_ASEWKT,
    ST_ASGEOJSON,
    ST_ASGML,
    ST_ASHEXEWKB,
    ST_ASKML,
    ST_ASSVG,
    ST_ASTEXT,
    ST_AZIMUTH,
    ST_BOUNDARY,
    ST_BUFFER,
    ST_BUILDAREA,
    ST_CENTROID,
    ST_CLOSESTPOINT,
    ST_COLLECT,
    ST_COLLECTIONEXTRACT,
    ST_CONTAINS,
    ST_CONTAINSPROPERLY,
    ST_CONVEXHULL,
    ST_COORDDIM,
    ST_COVEREDBY,
    ST_COVERS,
    ST_CROSSES,
    ST_CURVETOLINE,
    ST_DFULLWITHIN,
    ST_DIFFERENCE,
    ST_DIMENSION,
    ST_DISJOINT,
    ST_DISTANCE,
    ST_DISTANCE_SPHERE,
    ST_DISTANCE_SPHEROID,
    ST_DWITHIN,
    ST_ENDPOINT,
    ST_ENVELOPE,
    ST_EQUALS,
    ST_EXTERIORRING,
    ST_FORCE_2D,
    ST_FORCE_3D,
    ST_FORCE_3DM,
    ST_FORCE_3DZ,
    ST_FORCE_4D,
    ST_FORCE_COLLECTION,
    ST_FORCERHR,
    ST_GEOHASH,
    ST_GEOMCOLLFROMTEXT,
    ST_GEOMFROMEWKB,
    ST_GEOMFROMEWKT,
    ST_GEOMFROMGEOJSON,
    ST_GEOMFROMGML,
    ST_GEOMFROMKML,
    ST_GEOMFROMTEXT,
    ST_GEOMFROMWKB,
    ST_GEOMETRYFROMTEXT,
    ST_GEOMETRYN,
    ST_GEOMETRYTYPE,
    ST_GMLTOSQL,
    ST_HASARC,
    ST_HAUSDORFFDISTANCE,
    ST_INTERIORRINGN,
    ST_INTERSECTION,
    ST_INTERSECTS,
    ST_ISCLOSED,
    ST_ISEMPTY,
    ST_ISRING,
    ST_ISSIMPLE,
    ST_ISVALID,
    ST_ISVALIDREASON,
    ST_LENGTH,
    ST_LENGTH2D,
    ST_LENGTH3D,
    ST_LINECROSSINGDIRECTION,
    ST_LINEFROMMULTIPOINT,
    ST_LINEFROMTEXT,
    ST_LINEFROMWKB,
    ST_LINEMERGE,
    ST_LINESTRINGFROMWKB,
    ST_LINETOCURVE,
    ST_LINE_INTERPOLATE_POINT,
    ST_LINE_LOCATE_POINT,
    ST_LINE_SUBSTRING,
    ST_LONGESTLINE,
    ST_M,
    ST_MAKEENVELOPE,
    ST_MAKELINE,
    ST_MAKEPOINT,
    ST_MAKEPOINTM,
    ST_MAKEPOLYGON,
    ST_MAXDISTANCE,
    ST_MEM_SIZE,
    ST_MINIMUMBOUNDINGCIRCLE,
    ST_MULTI,
    ST_NDIMS,
    ST_NPOINTS,
    ST_NRINGS,
    ST_NUMGEOMETRIES,
    ST_NUMINTERIORRING,
    ST_NUMINTERIORRINGS,
    ST_NUMPOINTS,
    ST_ORDERINGEQUALS,
    ST_OVERLAPS,
    ST_PERIMETER,
    ST_PERIMETER2D,
    ST_PERIMETER3D,
    ST_POINT,
    ST_POINTFROMTEXT,
    ST_POINTFROMWKB,
    ST_POINTN,
    ST_POINTONSURFACE,
    ST_POINT_INSIDE_CIRCLE,
    ST_POLYGON,
    ST_POLYGONFROMTEXT,
    ST_POLYGONIZE,
    ST_RELATE,
    ST_REMOVEPOINT,
    ST_REVERSE,
    ST_ROTATE,
    ST_ROTATEX,
    ST_ROTATEY,
    ST_ROTATEZ,
    ST_SCALE,
    ST_SEGMENTIZE,
    ST_SETPOINT,
    ST_SETSRID,
    ST_SHIFT_LONGITUDE,
    ST_SHORTESTLINE,
    ST_SIMPLIFY,
    ST_SIMPLIFYPRESERVETOPOLOGY,
    ST_SNAPTOGRID,
    ST_SRID,
    ST_STARTPOINT,
    ST_SUMMARY,
    ST_SYMDIFFERENCE,
    ST_TOUCHES,
    ST_TRANSFORM,
    ST_TRANSLATE,
    ST_TRANSSCALE,
    ST_UNION,
    ST_WITHIN,
    ST_WKBTOSQL,
    ST_WKTTOSQL,
    ST_X,
    ST_XMAX,
    ST_XMIN,
    ST_Y,
    ST_YMAX,
    ST_YMIN,
    ST_Z,
    ST_ZMAX,
    ST_ZMIN,
    ST_ZMFLAG,
    STR_TO_DATE,
    STRCMP,
    STRFTIME,
    STRPOS,
    STRICT,
    STRING_AGG,
    STRING_TO_ARRAY,
    STRING_TO_TABLE,
    STRING,
    STRIP,
    SUBCLASS_ORIGIN,
    SUBDATE,
    SUBJECT,
    SUBPARTITION,
    SUBPARTITIONS,
    SUBSTR,
    SUBSTRING_INDEX,
    SUBSTRING,
    SUBTIME,
    SUM,
    SUMMARY,
    SUPER,
    SUPERUSER,
    SUSPEND,
    SWAPS,
    SWITCHES,
    SYSDATE,
    SYSID,
    SYSTEM_TIME,
    SYSTEM_USER,
    SYSTEM,
    TABLE_CHECKSUM,
    TABLE_NAME,
    TABLE_TO_XML,
    TABLE_TO_XML_AND_XMLSCHEMA,
    TABLE_TO_XMLSCHEMA,
    TABLES,
    TABLESPACE,
    TAN,
    TAND,
    TANH,
    TEMP,
    TEMPORARY,
    TEMPTABLE,
    TEXT_PATTERN_OPS,
    TEXT,
    THAN,
    THREADS,
    TIES,
    TIME_FORMAT,
    TIME_TO_SEC,
    TIME_ZONE,
    TIME,
    TIMEDIFF,
    TIMESTAMP,
    TIMESTAMPADD,
    TIMESTAMPDIFF,
    TIMESTAMPTZ,
    TIMETZ,
    TIMING,
    TO_ASCII,
    TO_BASE64,
    TO_BIN,
    TO_CHAR,
    TO_DATE,
    TO_DAYS,
    TO_HEX,
    TO_NUMBER,
    TO_OCT,
    TO_SECONDS,
    TO_TIMESTAMP,
    TO_TSQUERY,
    TO_TSVECTOR,
    TRANSACTIONAL,
    TRANSLATE,
    TRIGGERS,
    TRIM,
    TRIM_SCALE,
    TRUNCATE,
    TSMULTIRANGE,
    TSQUERY,
    TSQUERY_PHRASE,
    TSRANGE,
    TSTZMULTIRANGE,
    TSTZRANGE,
    TSVECTOR,
    TSVECTOR_TO_ARRAY,
    TS_DEBUG,
    TS_DELETE,
    TS_FILTER,
    TS_HEADLINE,
    TS_LEXIZE,
    TS_PARSE,
    TS_RANK,
    TS_RANK_CD,
    TS_REWRITE,
    TS_STAT,
    TS_TOKEN_TYPE,
    TYPE,
    TYPES,
    UCASE,
    UNBOUNDED,
    UNCOMMITTED,
    UNCOMPRESS,
    UNCOMPRESSED_LENGTH,
    UNDEFINED,
    UNDO_BUFFER_SIZE,
    UNDOFILE,
    UNHEX,
    UNICODE,
    UNICODE_ASSIGNED,
    UNISTR,
    UNINSTALL,
    UNIX_TIMESTAMP,
    UNKNOWN,
    UNNEST,
    UNTIL,
    UPDATEXML,
    UPGRADE,
    UPPER,
    USE_FRM,
    USER_RESOURCES,
    USER,
    UUID_EXTRACT_TIMESTAMP,
    UUID_EXTRACT_VERSION,
    UUID_SHORT,
    UUID_TO_BIN,
    UUID,
    UUIDV4,
    UUIDV7,
    VALID,
    VALIDATE_PASSWORD_STRENGTH,
    VALIDATE,
    VALUE,
    VAR_POP,
    VAR_SAMP,
    VARBIT,
    VARCHAR_PATTERN_OPS,
    VARCHAR2,
    VARIABLES,
    VARIANCE,
    VERBOSE,
    VERSION,
    VERSIONING,
    VIA,
    VIEW,
    VIRTUAL,
    VISIBLE,
    VOLATILE,
    WAIT,
    WAL,
    WARNING,
    WARNINGS,
    WEBSEARCH_TO_TSQUERY,
    WEEK,
    WEEKDAY,
    WEEKOFYEAR,
    WEIGHT_STRING,
    WIDTH,
    WIDTH_BUCKET,
    WITHIN,
    WITHOUT,
    WORK,
    WRAPPER,
    X509,
    XA,
    XML,
    XML_IS_WELL_FORMED,
    XML_IS_WELL_FORMED_CONTENT,
    XML_IS_WELL_FORMED_DOCUMENT,
    XMLAGG,
    XMLCOMMENT,
    XMLCONCAT,
    XMLTABLE,
    XMLTEXT,
    XPATH,
    XPATH_EXISTS,
    YAML,
    YEAR,
    YEARWEEK,
    ZONE,
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
