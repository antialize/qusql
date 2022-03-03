from typing import Any, Callable, Final, List, Optional
from mypy.plugin import (
    Plugin,
    MethodSigContext,
    CheckerPluginInterface,
    FunctionSigContext,
    FunctionContext,
)
from mypy.types import (
    Type,
    CallableType,
    AnyType,
    TypeOfAny,
    UnionType,
    NoneType,
    TupleType,
    is_named_instance,
    Instance,
)
from mypy.nodes import StrExpr, OpExpr, Expression, Context
from mypy.errorcodes import ErrorCode
import re
import mysql_type_plugin.mysql_type_plugin as rs  # type: ignore


def get_str_value(e: Expression) -> Optional[str]:
    if isinstance(e, StrExpr):
        return e.value
    if isinstance(e, OpExpr):
        if e.op == "+":
            l = get_str_value(e.left)
            r = get_str_value(e.right)
            if l is not None and r is not None:
                return l + r
    return None


DYNAMIC_SQL = ErrorCode(
    "dynamic-sql", "Query not of string literal", "SQL"
)  # type: Final

USE_DB_EXECUTE = ErrorCode("use-db-execute", "Use db_execute", "SQL")  # type: Final

schemas = None


def get_schemas(api: CheckerPluginInterface, context: Context) -> Any:
    global schemas
    if schemas is not None:
        return schemas
    try:
        src = open("mysql-type-schema.sql").read()
    except Exception as e:
        api.fail(f"Unable to read mysql-type-schema.sql: {e}", context)
        return None

    (s, err, message) = rs.parse_schemas("mysql-type-schema.sql", src)
    if err:
        api.fail(message, context)
    elif message:
        if note := getattr(api, "note"):
            note(message, context)
    schemas = s
    return schemas


def get_sql(
    sql_arg: int,
    args: List[List[Expression]],
    api: CheckerPluginInterface,
    quiet: bool = False,
) -> Optional[str]:
    if len(args) <= sql_arg:
        return None
    if len(args[sql_arg]) < 1:
        return None
    sql_arg_val = args[sql_arg][0]
    sql = get_str_value(sql_arg_val)
    if sql is None:
        if not quiet and (note := getattr(api, "note")):
            note("Dynamic sql", sql_arg_val, code=DYNAMIC_SQL)
        return None
    return sql


def type_statement(
    sql: str, api: CheckerPluginInterface, context: Context, quiet: bool = False
) -> Optional[Any]:
    schemas = get_schemas(api, context)
    if schemas is None:
        return None
    try:
        a = rs.type_statement(schemas, sql)
    except Exception as e:
        api.fail(f"ICE {sql} {e}", context)
        return None
    if len(a) != 3:
        api.fail(f"ICE {a}", context)
        return None

    (stmt, err, message) = a
    if err and not quiet:
        api.fail(message, context)
    elif message and not quiet:
        if note := getattr(api, "note"):
            note(message, context)
    return stmt


def get_argument_types(
    stmt: Any, api: CheckerPluginInterface, context: Context
) -> List[Type]:
    ts: List[Type] = []
    if args := getattr(stmt, "arguments", None):
        for k, (v, not_null) in args.items():
            if not isinstance(k, int):
                continue
            while len(ts) <= k:
                ts.append(AnyType(TypeOfAny.special_form))
            t: Type = AnyType(TypeOfAny.special_form)
            if isinstance(v, rs.Integer):
                t = api.named_generic_type("int", [])
            elif isinstance(v, rs.Float):
                t = api.named_generic_type("float", [])
            elif isinstance(v, rs.String):
                t = api.named_generic_type("str", [])
            elif isinstance(v, rs.Bool):
                t = api.named_generic_type("bool", [])
            elif isinstance(v, rs.Bytes):
                t = api.named_generic_type("bytes", [])
            elif isinstance(v, rs.Enum):
                t = api.named_generic_type("str", [])  # TODO literal with values
            elif isinstance(v, rs.Any):
                t = AnyType(TypeOfAny.special_form)
            else:
                api.fail(f"Unknown type {v}", context)
            if not not_null:
                t = UnionType((t, NoneType()))
            ts[k] = t
    return ts


class CustomPlugin(Plugin):
    def get_function_signature_hook(
        self, fullname: str
    ) -> Optional[Callable[[FunctionSigContext], CallableType]]:
        if fullname == "mysql_type.execute":
            many = False
        elif fullname == "mysql_type.execute_many":
            many = True
        else:
            return None

        def execute_hook(context: FunctionSigContext) -> CallableType:
            sql = get_sql(1, context.args, context.api, quiet=True)
            if sql is None:
                return context.default_signature

            sql2 = sql.replace("_LIST_", "%s, %s, %s")
            stmt = type_statement(sql2, context.api, context.context, quiet=True)
            if stmt is None:
                return context.default_signature
            ans = CallableType(
                [
                    context.default_signature.arg_types[0],
                    context.api.named_generic_type("str", []),
                ],
                [0, 0],
                ["cursor", "sql"],
                context.default_signature.ret_type,
                context.default_signature.fallback,
                variables=context.default_signature.variables,
            )

            at = get_argument_types(stmt, context.api, context.context)

            ati = iter(at)
            for i, a in enumerate(re.findall("(_LIST_|[%]s|[%]\()", sql)):
                try:
                    if a == "_LIST_":
                        t = context.api.named_generic_type(
                            "typing.Sequence", [next(ati)]
                        )
                        next(ati)
                        next(ati)
                    else:
                        t = next(ati)
                    ans.arg_types.append(t)
                    ans.arg_names.append(f"a{i}")
                    ans.arg_kinds.append(0)
                except StopIteration:
                    context.api.fail(f"Argument match failed {i}", context.context)
            return ans

        return execute_hook

    def get_function_hook(
        self, fullname: str
    ) -> Optional[Callable[[FunctionContext], Type]]:
        if fullname == "mysql_type.execute":
            many = False
        elif fullname == "mysql_type.executemany":
            many = True
        else:
            return None

        def execute_hook(context: FunctionContext) -> Type:
            api = context.api
            ct = context.arg_types[0][0]
            if ct.type.has_base("MySQLdb.cursors.DictCursor"):
                dc = True
                context.api.fail("Dict cursors not supported", ct)
            elif ct.type.has_base("MySQLdb.cursors.Cursor"):
                dc = False
            else:
                context.api.fail(f"Unknown cursor {ct}", ct)
            sql = get_sql(1, context.args, api, quiet=False)
            if sql is None:
                return context.default_return_type
            sql = sql.replace("_LIST_", "%s")
            stmt = type_statement(sql, api, context.context, quiet=False)
            if stmt is None:
                return context.default_return_type

            if isinstance(stmt, rs.Select):
                ts: List[Type] = []
                for (name, type_, not_null) in stmt.columns:
                    t: Type
                    if isinstance(type_, rs.Integer):
                        t = api.named_generic_type("int", [])
                    elif isinstance(type_, rs.Float):
                        t = api.named_generic_type("float", [])
                    elif isinstance(type_, rs.String):
                        t = api.named_generic_type("str", [])
                    elif isinstance(type_, rs.Bool):
                        t = api.named_generic_type("int", [])  # TODO literal 0,1
                    elif isinstance(type_, rs.Enum):
                        t = api.named_generic_type(
                            "str", []
                        )  # TODO literal with values
                    elif isinstance(type_, rs.Bytes):
                        t = api.named_generic_type("bytes", [])
                    elif isinstance(type_, rs.Any):
                        t = AnyType(TypeOfAny.special_form)
                    else:
                        api.fail(f"Unknown type {type_}", context.context)
                    if not not_null:
                        t = UnionType((t, NoneType()))
                    ts.append(t)
                if sr := self.lookup_fully_qualified("mysql_type.SelectResult"):
                    return Instance(
                        sr.node,  # type: ignore
                        [TupleType(ts, api.named_generic_type("tuple", []))],
                    )
                else:
                    context.api.fail(f"Could not find mysql_type.SelectResult", ct)

            elif isinstance(stmt, rs.Insert):
                if stmt.yield_autoincrement == "yes":
                    if ir := self.lookup_fully_qualified(
                        "mysql_type.InsertWithLastRowIdResult"
                    ):
                        return Instance(ir.node, [])  # type: ignore
                elif stmt.yield_autoincrement == "maybe":
                    if ir := self.lookup_fully_qualified(
                        "mysql_type.InsertWithOptLastRowIdResult"
                    ):
                        return Instance(ir.node, [])  # type: ignore
            if other := self.lookup_fully_qualified("mysql_type.OtherResult"):
                return Instance(other.node, [])  # type: ignore
            return NoneType()

        return execute_hook

    def get_method_signature_hook(
        self, fullname: str
    ) -> Optional[Callable[[MethodSigContext], CallableType]]:
        if fullname in (
            "MySQLdb.cursors.Cursor.execute",
            "MySQLdb.cursors.DictCursor.execute",
        ):
            many = False
        elif fullname in (
            "MySQLdb.cursors.Cursor.executemany",
            "MySQLdb.cursors.DictCursor.executemany",
        ):
            many = True
        else:
            return None

        def execute_hook(context: MethodSigContext) -> CallableType:
            sql = get_sql(1, context.args, context.api, quiet=False)
            if sql is None:
                return context.default_signature
            stmt = type_statement(sql, context.api, context.context)
            if stmt is None:
                return context.default_signature
            ans = CallableType(
                [
                    context.api.named_generic_type("str", []),
                    AnyType(TypeOfAny.special_form),
                ],
                [0, 1],
                ["sql", "arguments"],
                context.default_signature.ret_type,
                context.default_signature.fallback,
            )
            ts = get_argument_types(stmt, context.api, context.context)
            if many:
                ans.arg_types[1] = context.api.named_generic_type(
                    "list", [TupleType(ts, context.api.named_generic_type("tuple", []))]
                )
            else:
                ans.arg_types[1] = TupleType(
                    ts, context.api.named_generic_type("tuple", [])
                )
            if note := getattr(context.api, "note"):
                note("Use db_execute instead", context.context, code=USE_DB_EXECUTE)
            return ans

        return execute_hook


def plugin(version: str):
    # ignore version argument if the plugin works with all mypy versions.
    return CustomPlugin
