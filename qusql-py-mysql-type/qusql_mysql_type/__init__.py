import re
from typing import Any, Iterable, Iterator, Optional, Protocol, TypeVar, cast, List
import MySQLdb  # type: ignore[import-untyped]
import MySQLdb.cursors  # type: ignore[import-untyped]

CursorType = TypeVar("CursorType", bound=MySQLdb.cursors.BaseCursor)
T = TypeVar("T")


class SelectResult(Protocol[T]):
    rowcount: int

    def fetchone(self) -> Optional[T]:
        """
        Fetches a single row from the cursor. None indicates that
        no more rows are available.
        """

    def fetchall(self) -> List[T]:
        """Fetchs all available rows from the cursor."""

    def fetchmany(self, size: Optional[int] = None) -> List[T]:
        """
        Fetch up to size rows from the cursor. Result set may be smaller
        than size. If size is not defined, cursor.arraysize is used.
        """

    def __iter__(self) -> Iterator[T]: ...


class OtherResult:
    rowcount: int


class InsertWithLastRowIdResult:
    rowcount: int
    lastrowid: int


class InsertWithOptLastRowIdResult:
    rowcount: int
    lastrowid: Optional[int]


class UntypedResult:
    """
    Return type of execute, if the mysql-type-plugin is not used by mypy
    """

    rowcount: int
    lastrowid: Optional[int]

    def fetchone(self) -> Optional[Any]:
        """
        Fetches a single row from the cursor. None indicates that
        no more rows are available.
        """

    def fetchall(self) -> List[Any]:
        """Fetchs all available rows from the cursor."""

    def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        """
        Fetch up to size rows from the cursor. Result set may be smaller
        than size. If size is not defined, cursor.arraysize is used.
        """

    def __iter__(self) -> Iterator[Any]: ...


def execute(c: CursorType, sql: str, *args: Any) -> UntypedResult:
    """
    Call c.execute, with the given sqlstatement and argumens, and return c.

    The mysql-type-plugin for mysql will special type this function such
    that the number and types of args matches the what is expected for the query
    by analyzing the mysql-schema.sql file in the project root.

    The return type will be changed to either: InsertResult, OtherResult or
    SelectResult depending on the query type.
    For select queries the SelectResult will be typed with either a Tuple with
    arguments with Tuple or TypedDict determined from the sql. Based on wether
    the curser is a dict cursor or not.

    Arguments in the query are expected to be presented as %s or _LIST_
    . For list arguments the coresponding entry in args is assumed to be a list
    or a tuple and the _LIST_ in the query is replaced by %s,%s,...,%s with where
    the number of $s's is equal to the list length
    """
    if "_LIST_" not in sql:
        c.execute(sql, args)
    else:
        arg_iter = iter(args)
        flatargs: List[Any] = []

        def replace_arg(mo: re.Match[str]) -> str:
            # Consume scalar args into flatargs until we reach the list arg
            # for this _LIST_ placeholder.
            for a in arg_iter:
                if isinstance(a, tuple):
                    a = list(a)
                if isinstance(a, list):
                    flatargs.extend(a)
                    if not a:
                        # Empty list: IN (NULL) matches no rows for any value.
                        return "null"
                    return ", ".join(["%s"] * len(a))
                flatargs.append(a)
            raise Exception("No list argument found for _LIST_")

        sql = re.sub("_LIST_", replace_arg, sql)
        # Consume any remaining scalar args that follow the last _LIST_.
        for a in arg_iter:
            if isinstance(a, list | tuple):
                raise Exception("Number of _LIST_ arguments do not match")
            flatargs.append(a)
        c.execute(sql, flatargs)
    return cast(UntypedResult, c)


def executemany(c: CursorType, sql: str, args: Iterable[Iterable[Any]]) -> OtherResult:
    """
    Call c.executemany, with the given sqlstatement and argumens, and return c.

    The mysql-type-plugin for mysql will special type this function such
    that the number and types of args matches the what is expected for the query
    by analyzing the mysql-schema.sql file in the project root.
    """
    c.executemany(sql, args)
    return cast(OtherResult, c)
