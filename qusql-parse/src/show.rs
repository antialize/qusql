use alloc::boxed::Box;

use crate::{
    SString, Span, Spanned, Statement,
    expression::{Expression, parse_expression_unrestricted},
    keywords::Keyword,
    lexer::Token,
    parser::{ParseError, Parser},
    qualified_name::parse_qualified_name, restrict::Restrict,
};

/// Parse result for `SHOW TABLES` variants
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "SHOW EXTENDED TABLES FROM test_db LIKE 't%';";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// match stmts.pop() {
///     Some(Statement::ShowTables(s)) => {
///         // inspect s.extended, s.db, s.pattern, etc.
///     }
///     _ => panic!("expected ShowTables"),
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ShowTables<'a> {
    pub show_span: Span,
    pub tables_span: Span,
    pub extended: Option<Span>,
    pub full: Option<Span>,
    pub db: Option<crate::QualifiedName<'a>>,
    pub like: Option<SString<'a>>,
    pub where_expr: Option<Expression<'a>>,
}

impl<'a> Spanned for ShowTables<'a> {
    fn span(&self) -> Span {
        self.show_span
            .join_span(&self.tables_span)
            .join_span(&self.extended)
            .join_span(&self.full)
            .join_span(&self.db)
            .join_span(&self.like)
            .join_span(&self.where_expr)
    }
}

fn parse_show_tables<'a>(
    parser: &mut Parser<'a, '_>,
    show_span: Span,
    extended: Option<Span>,
    full: Option<Span>,
) -> Result<ShowTables<'a>, ParseError> {
    let tables_span = parser.consume_keyword(Keyword::TABLES)?;

    // optional FROM or IN db_name
    let mut db = None;
    match &parser.token {
        Token::Ident(_, Keyword::FROM) => {
            parser.consume_keyword(Keyword::FROM)?;
            // Only restrict LIKE and WHERE, which can follow the db name
            let q = parse_qualified_name(parser, &Restrict::new(&[Keyword::LIKE, Keyword::WHERE]))?;
            db = Some(q);
        }
        Token::Ident(_, Keyword::IN) => {
            parser.consume_keyword(Keyword::IN)?;
            let q = parse_qualified_name(parser, &Restrict::new(&[Keyword::LIKE, Keyword::WHERE]))?;
            db = Some(q);
        }
        _ => {}
    }

    // optional LIKE or WHERE
    let like = if parser.skip_keyword(Keyword::LIKE).is_some() {
        Some(parser.consume_string()?)
    } else {
        None
    };
    let where_expr = if like.is_none() && parser.skip_keyword(Keyword::WHERE).is_some() {
        Some(parse_expression_unrestricted(parser, false)?)
    } else {
        None
    };

    Ok(ShowTables {
        show_span,
        tables_span,
        extended,
        full,
        db,
        like,
        where_expr,
    })
}

/// Parse result for `SHOW DATABASES`
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "SHOW DATABASES;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// match stmts.pop() {
///     Some(Statement::ShowDatabases(_)) => {}
///     _ => panic!("expected ShowDatabases"),
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ShowDatabases {
    pub show_span: Span,
    pub databases_span: Span,
}

impl Spanned for ShowDatabases {
    fn span(&self) -> Span {
        self.show_span.clone().join_span(&self.databases_span)
    }
}

fn parse_show_databases<'a>(
    parser: &mut Parser<'a, '_>,
    show_span: Span,
) -> Result<ShowDatabases, ParseError> {
    let databases_span = parser.consume_keyword(Keyword::DATABASES)?;
    Ok(ShowDatabases {
        show_span,
        databases_span,
    })
}

/// Parse result for `SHOW PROCESSLIST` / `SHOW FULL PROCESSLIST`
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "SHOW FULL PROCESSLIST;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// match stmts.pop() {
///     Some(Statement::ShowProcessList(_)) => {}
///     _ => panic!("expected ShowProcessList"),
/// }
/// ```

#[derive(Clone, Debug)]
pub struct ShowProcessList {
    pub show_span: Span,
    pub process_span: Span,
}

impl Spanned for ShowProcessList {
    fn span(&self) -> Span {
        self.show_span.clone().join_span(&self.process_span)
    }
}

fn parse_show_processlist<'a>(
    parser: &mut Parser<'a, '_>,
    show_span: Span,
    _full: Option<Span>,
) -> Result<ShowProcessList, ParseError> {
    match &parser.token {
        Token::Ident(_, Keyword::PROCESSLIST) => {
            let process_span = parser.consume_keyword(Keyword::PROCESSLIST)?;
            Ok(ShowProcessList {
                show_span,
                process_span,
            })
        }
        Token::Ident(_, Keyword::PROCESS) => {
            let process_span = parser.consume_keyword(Keyword::PROCESS)?;
            Ok(ShowProcessList {
                show_span,
                process_span,
            })
        }
        _ => parser.expected_failure("'PROCESS' | 'PROCESSLIST'"),
    }
}

/// Parse result for `SHOW VARIABLES`
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "SHOW VARIABLES LIKE 'max_%';";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// match stmts.pop() {
///     Some(Statement::ShowVariables(s)) => {
///         // s.pattern contains the LIKE expression
///     }
///     _ => panic!("expected ShowVariables"),
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ShowVariables<'a> {
    pub show_span: Span,
    pub variables_span: Span,
    pub global_span: Option<Span>,
    pub session_span: Option<Span>,
    pub like: Option<SString<'a>>,
    pub where_expr: Option<Expression<'a>>,
}

impl<'a> Spanned for ShowVariables<'a> {
    fn span(&self) -> Span {
        self.show_span
            .join_span(&self.variables_span)
            .join_span(&self.global_span)
            .join_span(&self.session_span)
            .join_span(&self.like)
            .join_span(&self.where_expr)
    }
}

fn parse_show_variables<'a>(
    parser: &mut Parser<'a, '_>,
    show_span: Span,
    global_span: Option<Span>,
    session_span: Option<Span>,
) -> Result<ShowVariables<'a>, ParseError> {
    let variables_span = parser.consume_keyword(Keyword::VARIABLES)?;
    let like = if parser.skip_keyword(Keyword::LIKE).is_some() {
        Some(parser.consume_string()?)
    } else {
        None
    };
    let where_expr = if parser.skip_keyword(Keyword::WHERE).is_some() {
        Some(parse_expression_unrestricted(parser, false)?)
    } else {
        None
    };
    Ok(ShowVariables {
        show_span,
        variables_span,
        global_span,
        session_span,
        like,
        where_expr,
    })
}

/// Parse result for `SHOW STATUS`
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "SHOW STATUS LIKE 'Threads%';";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// match stmts.pop() {
///     Some(Statement::ShowStatus(_)) => {}
///     _ => panic!("expected ShowStatus"),
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ShowStatus<'a> {
    pub show_span: Span,
    pub status_span: Span,
    pub global_span: Option<Span>,
    pub session_span: Option<Span>,
    pub like: Option<SString<'a>>,
    pub where_expr: Option<Expression<'a>>,
}

impl<'a> Spanned for ShowStatus<'a> {
    fn span(&self) -> Span {
        self.show_span
            .join_span(&self.status_span)
            .join_span(&self.like)
            .join_span(&self.where_expr)
    }
}

fn parse_show_status<'a>(
    parser: &mut Parser<'a, '_>,
    show_span: Span,
    global_span: Option<Span>,
    session_span: Option<Span>,
) -> Result<ShowStatus<'a>, ParseError> {
    let status_span = parser.consume_keyword(Keyword::STATUS)?;
    let like = if parser.skip_keyword(Keyword::LIKE).is_some() {
        Some(parser.consume_string()?)
    } else {
        None
    };
    let where_expr = if parser.skip_keyword(Keyword::WHERE).is_some() {
        Some(parse_expression_unrestricted(parser, false)?)
    } else {
        None
    };
    Ok(ShowStatus {
        show_span,
        status_span,
        global_span,
        session_span,
        like,
        where_expr,
    })
}

/// Parse result for `SHOW COLUMNS` / `SHOW FIELDS`
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "SHOW COLUMNS FROM `my_table` LIKE 'id%';";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// match stmts.pop() {
///     Some(Statement::ShowColumns(c)) => {
///         // c.table contains the table name
///     }
///     _ => panic!("expected ShowColumns"),
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ShowColumns<'a> {
    pub show_span: Span,
    pub columns_span: Span,
    pub extended: Option<Span>,
    pub full: Option<Span>,
    pub table: Option<crate::QualifiedName<'a>>,
    pub db: Option<crate::QualifiedName<'a>>,
    pub like: Option<SString<'a>>,
    pub where_expr: Option<Expression<'a>>,
}

impl<'a> Spanned for ShowColumns<'a> {
    fn span(&self) -> Span {
        self.show_span
            .join_span(&self.columns_span)
            .join_span(&self.table)
            .join_span(&self.db)
            .join_span(&self.like)
            .join_span(&self.where_expr)
    }
}

fn parse_show_columns<'a>(
    parser: &mut Parser<'a, '_>,
    show_span: Span,
    extended: Option<Span>,
    full: Option<Span>,
) -> Result<ShowColumns<'a>, ParseError> {
    let columns_span = match &parser.token {
        Token::Ident(_, Keyword::COLUMNS) => parser.consume_keyword(Keyword::COLUMNS)?,
        _ => parser.consume_keyword(Keyword::FIELDS)?,
    };
    let mut table = None;
    let mut db = None;
    // Restrict LIKE and WHERE after table/db names
    let restrict_like_where = Restrict::new(&[Keyword::LIKE, Keyword::WHERE]);
    if parser.skip_keyword(Keyword::FROM).is_some() || parser.skip_keyword(Keyword::IN).is_some() {
        let q = parse_qualified_name(parser, &restrict_like_where)?;
        table = Some(q);
    }
    // optional second FROM/IN specifying database: SHOW COLUMNS FROM tbl FROM db
    if table.is_some() {
        match &parser.token {
            Token::Ident(_, Keyword::FROM) => {
                parser.consume_keyword(Keyword::FROM)?;
                let q = parse_qualified_name(parser, &restrict_like_where)?;
                db = Some(q);
            }
            Token::Ident(_, Keyword::IN) => {
                parser.consume_keyword(Keyword::IN)?;
                let q = parse_qualified_name(parser, &restrict_like_where)?;
                db = Some(q);
            }
            _ => {}
        }
    }
    let like = if parser.skip_keyword(Keyword::LIKE).is_some() {
        Some(parser.consume_string()?)
    } else {
        None
    };
    let where_expr = if like.is_none() && parser.skip_keyword(Keyword::WHERE).is_some() {
        Some(parse_expression_unrestricted(parser, false)?)
    } else {
        None
    };
    Ok(ShowColumns {
        show_span,
        columns_span,
        extended,
        full,
        table,
        db,
        like,
        where_expr,
    })
}

/// Parse result for `SHOW CHARACTER SET` / `SHOW CHARSET`
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "SHOW CHARACTER SET WHERE Charset LIKE 'utf%';";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// match stmts.pop() {
///     Some(Statement::ShowCharacterSet(s)) => {
///         // s.where_expr contains the WHERE expression; s.pattern contains the LIKE pattern when used directly
///     }
///     _ => panic!("expected ShowCharacterSet"),
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ShowCharacterSet<'a> {
    pub show_span: Span,
    pub character_span: Option<Span>,
    pub set_span: Span,
    pub like: Option<SString<'a>>,
    pub where_expr: Option<Expression<'a>>,
}

impl<'a> Spanned for ShowCharacterSet<'a> {
    fn span(&self) -> Span {
        self.show_span
            .join_span(&self.character_span)
            .join_span(&self.set_span)
            .join_span(&self.like)
            .join_span(&self.where_expr)
    }
}

fn parse_show_character_set<'a>(
    parser: &mut Parser<'a, '_>,
    show_span: Span,
) -> Result<ShowCharacterSet<'a>, ParseError> {
    // Accept either: SHOW CHARSET ...  or SHOW CHARACTER SET ...
    let mut character_span: Option<Span> = None;
    let set_span = match &parser.token {
        Token::Ident(_, Keyword::CHARSET) => parser.consume_keyword(Keyword::CHARSET)?,
        Token::Ident(_, Keyword::CHARACTER) => {
            character_span = Some(parser.consume_keyword(Keyword::CHARACTER)?);
            parser.consume_keyword(Keyword::SET)?
        }
        _ => return parser.expected_failure("'CHARSET' | 'CHARACTER'"),
    };

    let mut like: Option<SString<'a>> = None;
    let mut where_expr: Option<Expression<'a>> = None;
    if parser.skip_keyword(Keyword::LIKE).is_some() {
        like = Some(parser.consume_string()?);
    } else if parser.skip_keyword(Keyword::WHERE).is_some() {
        where_expr = Some(parse_expression_unrestricted(parser, false)?);
    }

    Ok(ShowCharacterSet {
        show_span,
        character_span,
        set_span,
        like,
        where_expr,
    })
}

/// Parse result for `SHOW CREATE TABLE`
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "SHOW CREATE TABLE my_table;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// match stmts.pop() {
///     Some(Statement::ShowCreateTable(s)) => {
///         // s.table contains the table name
///     }
///     _ => panic!("expected ShowCreateTable"),
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ShowCreateTable<'a> {
    pub show_span: Span,
    pub create_span: Span,
    pub object_span: Span,
    pub table: crate::QualifiedName<'a>,
}

impl<'a> Spanned for ShowCreateTable<'a> {
    fn span(&self) -> Span {
        self.show_span
            .join_span(&self.create_span)
            .join_span(&self.object_span)
            .join_span(&self.table)
    }
}

/// Parse result for `SHOW CREATE DATABASE`
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "SHOW CREATE DATABASE my_db;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// match stmts.pop() {
///     Some(Statement::ShowCreateDatabase(s)) => {
///         // s.db contains the database name
///     }
///     _ => panic!("expected ShowCreateDatabase"),
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ShowCreateDatabase<'a> {
    pub show_span: Span,
    pub create_span: Span,
    pub object_span: Span,
    pub db: crate::QualifiedName<'a>,
}

impl<'a> Spanned for ShowCreateDatabase<'a> {
    fn span(&self) -> Span {
        self.show_span
            .join_span(&self.create_span)
            .join_span(&self.object_span)
            .join_span(&self.db)
    }
}

/// Parse result for `SHOW CREATE VIEW`
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "SHOW CREATE VIEW my_view;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// match stmts.pop() {
///     Some(Statement::ShowCreateView(s)) => {
///         // s.view contains the view name
///     }
///     _ => panic!("expected ShowCreateView"),
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ShowCreateView<'a> {
    pub show_span: Span,
    pub create_span: Span,
    pub object_span: Span,
    pub view: crate::QualifiedName<'a>,
}

impl<'a> Spanned for ShowCreateView<'a> {
    fn span(&self) -> Span {
        self.show_span
            .join_span(&self.create_span)
            .join_span(&self.object_span)
            .join_span(&self.view)
    }
}

fn parse_show_create<'a>(
    parser: &mut Parser<'a, '_>,
    show_span: Span,
) -> Result<crate::Statement<'a>, ParseError> {
    let create_span = parser.consume_keyword(Keyword::CREATE)?;
    use crate::qualified_name::parse_qualified_name_unrestricted;
    match &parser.token {
        Token::Ident(_, Keyword::TABLE) => {
            let object_span = parser.consume_keyword(Keyword::TABLE)?;
            let table = parse_qualified_name_unrestricted(parser)?;
            Ok(crate::Statement::ShowCreateTable(Box::new(
                ShowCreateTable {
                    show_span,
                    create_span,
                    object_span,
                    table,
                },
            )))
        }
        Token::Ident(_, Keyword::DATABASE) => {
            let object_span = parser.consume_keyword(Keyword::DATABASE)?;
            let db = parse_qualified_name_unrestricted(parser)?;
            Ok(crate::Statement::ShowCreateDatabase(Box::new(
                ShowCreateDatabase {
                    show_span,
                    create_span,
                    object_span,
                    db,
                },
            )))
        }
        Token::Ident(_, Keyword::VIEW) => {
            let object_span = parser.consume_keyword(Keyword::VIEW)?;
            let view = parse_qualified_name_unrestricted(parser)?;
            Ok(crate::Statement::ShowCreateView(Box::new(ShowCreateView {
                show_span,
                create_span,
                object_span,
                view,
            })))
        }
        _ => parser.expected_failure("'TABLE' | 'DATABASE' | 'VIEW'"),
    }
}

/// Parse result for `SHOW COLLATION`
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "SHOW COLLATION LIKE 'utf%';";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// match stmts.pop() {
///     Some(Statement::ShowCollation(s)) => {
///         // s.pattern contains the LIKE string
///     }
///     _ => panic!("expected ShowCollation"),
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ShowCollation<'a> {
    pub show_span: Span,
    pub collation_span: Span,
    pub like: Option<SString<'a>>,
    pub where_expr: Option<Expression<'a>>,
}

impl<'a> Spanned for ShowCollation<'a> {
    fn span(&self) -> Span {
        self.show_span
            .join_span(&self.collation_span)
            .join_span(&self.like)
            .join_span(&self.where_expr)
    }
}

fn parse_show_collation<'a>(
    parser: &mut Parser<'a, '_>,
    show_span: Span,
) -> Result<ShowCollation<'a>, ParseError> {
    let collation_span = parser.consume_keyword(Keyword::COLLATION)?;
    let mut like: Option<SString<'a>> = None;
    let mut where_expr: Option<Expression<'a>> = None;
    if parser.skip_keyword(Keyword::LIKE).is_some() {
        like = Some(parser.consume_string()?);
    } else if parser.skip_keyword(Keyword::WHERE).is_some() {
        where_expr = Some(parse_expression_unrestricted(parser, false)?);
    }
    Ok(ShowCollation {
        show_span,
        collation_span,
        like,
        where_expr,
    })
}

/// Parse result for `SHOW ENGINES`
///
/// ```
/// # use qusql_parse::{SQLDialect, SQLArguments, ParseOptions, parse_statements, Statement, Issues};
/// # let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
/// let sql = "SHOW ENGINES;";
/// let mut issues = Issues::new(sql);
/// let mut stmts = parse_statements(sql, &mut issues, &options);
/// # assert!(issues.is_ok(), "{}", issues);
/// match stmts.pop() {
///     Some(Statement::ShowEngines(s)) => {
///         // s.engines_span is present
///     }
///     _ => panic!("expected ShowEngines"),
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ShowEngines {
    pub show_span: Span,
    pub engines_span: Span,
}

impl Spanned for ShowEngines {
    fn span(&self) -> Span {
        self.show_span.join_span(&self.engines_span)
    }
}

fn parse_show_engines<'a>(
    parser: &mut Parser<'a, '_>,
    show_span: Span,
) -> Result<ShowEngines, ParseError> {
    let engines_span = parser.consume_keyword(Keyword::ENGINES)?;
    Ok(ShowEngines {
        show_span,
        engines_span,
    })
}

pub(crate) fn parse_show<'a>(
    parser: &mut Parser<'a, '_>,
) -> Result<crate::Statement<'a>, ParseError> {
    let show_span = parser.consume_keyword(Keyword::SHOW)?;
    // parse optional modifiers EXTENDED, FULL, GLOBAL and SESSION (either or both) before dispatch
    let mut extended: Option<Span> = None;
    let mut full: Option<Span> = None;
    let mut global: Option<Span> = None;
    let mut session: Option<Span> = None;
    loop {
        match &parser.token {
            Token::Ident(_, Keyword::EXTENDED) => extended = Some(parser.consume()),
            Token::Ident(_, Keyword::FULL) => full = Some(parser.consume()),
            Token::Ident(_, Keyword::GLOBAL) => global = Some(parser.consume()),
            Token::Ident(_, Keyword::SESSION) => session = Some(parser.consume()),
            _ => break,
        }
    }

    let stmt = match &parser.token {
        Token::Ident(_, Keyword::TABLES) => Statement::ShowTables(Box::new(parse_show_tables(parser, show_span, extended.clone(), full.clone())?)),
        Token::Ident(_, Keyword::CREATE) => parse_show_create(parser, show_span)?,
        Token::Ident(_, Keyword::DATABASES) => Statement::ShowDatabases(Box::new(parse_show_databases(parser, show_span)?)),
        Token::Ident(_, Keyword::PROCESSLIST) | Token::Ident(_, Keyword::PROCESS) => {
            Statement::ShowProcessList(Box::new(parse_show_processlist(parser, show_span, full.clone())?))
        }
        Token::Ident(_, Keyword::VARIABLES) => Statement::ShowVariables(Box::new(parse_show_variables(parser, show_span, global.clone(), session.clone())?)),
        Token::Ident(_, Keyword::STATUS) => Statement::ShowStatus(Box::new(parse_show_status(parser, show_span, global.clone(), session.clone())?)),
        Token::Ident(_, Keyword::COLUMNS) | Token::Ident(_, Keyword::FIELDS) => {
            Statement::ShowColumns(Box::new(parse_show_columns(parser, show_span, extended.clone(), full.clone())?))
        }
        Token::Ident(_, Keyword::CHARSET) | Token::Ident(_, Keyword::CHARACTER) => {
            Statement::ShowCharacterSet(Box::new(parse_show_character_set(parser, show_span)?))
        }
            Token::Ident(_, Keyword::COLLATION) => Statement::ShowCollation(Box::new(parse_show_collation(parser, show_span)?)),
            Token::Ident(_, Keyword::ENGINES) => Statement::ShowEngines(Box::new(parse_show_engines(parser, show_span)?)),
        _ => return parser.expected_failure("'TABLES' | 'DATABASES' | 'PROCESS' | 'PROCESSLIST' | 'VARIABLES' | 'STATUS' | 'COLUMNS' | 'FIELDS' | 'CHARSET' | 'CHARACTER' | 'COLLATION' | 'ENGINES'"),
    };

    // Emit warnings for modifiers not supported by the particular SHOW variant
    if let Some(span) = &extended {
        match &stmt {
            crate::Statement::ShowTables(_) => {}
            crate::Statement::ShowColumns(_) => {}
            _ => {
                parser.warn(
                    "Modifier EXTENDED not supported for this SHOW variant",
                    span,
                );
            }
        }
    }

    if let Some(span) = &full {
        match &stmt {
            crate::Statement::ShowTables(_) => {}
            crate::Statement::ShowProcessList(_) => {}
            crate::Statement::ShowColumns(_) => {}
            _ => {
                parser.warn("Modifier FULL not supported for this SHOW variant", span);
            }
        }
    }

    if let Some(span) = &global {
        match &stmt {
            crate::Statement::ShowStatus(_) => {}
            crate::Statement::ShowVariables(_) => {}
            _ => {
                parser.warn("Modifier GLOBAL not supported for this SHOW variant", span);
            }
        }
    }

    if let Some(span) = &session {
        match &stmt {
            crate::Statement::ShowStatus(_) => {}
            crate::Statement::ShowVariables(_) => {}
            _ => {
                parser.warn("Modifier SESSION not supported for this SHOW variant", span);
            }
        }
    }

    Ok(stmt)
}
