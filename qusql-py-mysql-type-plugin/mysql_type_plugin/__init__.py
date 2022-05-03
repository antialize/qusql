from collections import OrderedDict
from typing import Any, Callable, Final, List, Optional, Tuple
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
    TypedDictType,
    ARG_POS
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
    sql: str, api: CheckerPluginInterface, context: Context, dict_cursor:bool, quiet: bool = False
) -> Optional[Any]:
    schemas = get_schemas(api, context)
    if schemas is None:
        return None
    try:
        a = rs.type_statement(schemas, sql, dict_cursor)
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

def map_type(v: Any, not_null: bool, api: CheckerPluginInterface, context: Context) -> Type:
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
    elif isinstance(v, rs.List):
        return api.named_generic_type(
            "typing.Sequence", [map_type(v.type, not_null, api, context)]
        )
    elif isinstance(v, rs.Any):
        t = AnyType(TypeOfAny.special_form)
    else:
        api.fail(f"Unknown type {v}", context)
    if not not_null:
        t = UnionType((t, NoneType()))
    return t

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
            ts[k] = map_type(v, not_null, api, context)
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
            try:
                sql = get_sql(1, context.args, context.api, quiet=True)
                if sql is None:
                    return context.default_signature

                stmt = type_statement(sql, context.api, context.context, dict_cursor=False, quiet=True)
                if stmt is None:
                    return context.default_signature
                ans = CallableType(
                    [
                        context.default_signature.arg_types[0],
                        context.api.named_generic_type("str", []),
                    ],
                    [ARG_POS, ARG_POS],
                    ["cursor", "sql"],
                    context.default_signature.ret_type,
                    context.default_signature.fallback,
                    variables=context.default_signature.variables,
                )

                at = get_argument_types(stmt, context.api, context.context)
                for i, t in enumerate(at):
                    ans.arg_types.append(t)
                    ans.arg_names.append(f"a{i}")
                    ans.arg_kinds.append(ARG_POS)
                return ans
            except Exception as e:
                context.api.fail(f"ICE: {e}", context.context)
                return context.default_signature

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
            try:
                api = context.api
                ct = context.arg_types[0][0]
                dc = False
                try:
                    if ct.type.has_base("MySQLdb.cursors.DictCursor"):
                        dc = True
                    elif ct.type.has_base("MySQLdb.cursors.Cursor"):
                        pass
                    else:
                        context.api.fail(f"Unknown cursor {ct}", context.context)
                except AttributeError:
                    context.api.fail(f"Unknown cursor {ct}", context.context)

                sql = get_sql(1, context.args, api, quiet=False)
                if sql is None:
                    return context.default_return_type
                stmt = type_statement(sql, api, context.context, dict_cursor=dc,quiet=False)
                if stmt is None:
                    return context.default_return_type

                if isinstance(stmt, rs.Select):
                    ntp: List[Tuple[str, Type]] = []
                    for (name, type_, not_null) in stmt.columns:
                        t = map_type(type_, not_null, api, context.context)
                        ntp.append((name, t))
                    if sr := self.lookup_fully_qualified("mysql_type.SelectResult"):
                        if dc:
                            return Instance(
                                sr.node, # type: ignore
                                [TypedDictType(
                                    OrderedDict(ntp),
                                    set([n for (n,_) in ntp]),
                                    api.named_generic_type("dict", [])
                                )]
                            )
                        else:
                            return Instance(
                                sr.node, # type: ignore
                                [TupleType([t for (_,t) in ntp], api.named_generic_type("tuple", []))],
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
            except Exception as e:
                context.api.fail(f"ICE: {e}", context.context)
                return context.default_return_type
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
            try:
                sql = get_sql(1, context.args, context.api, quiet=True)
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
                    [ARG_POS, ARG_POS],
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
            except Exception as e:
                context.api.fail(f"ICE: {e}", context.context)
                return context.default_signature
            return ans

        return execute_hook


def plugin(version: str):
    # ignore version argument if the plugin works with all mypy versions.
    return CustomPlugin
