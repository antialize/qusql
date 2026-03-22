# MySQL 8.4 Built-in Functions Reference

This file lists all built-in functions (not operators) from MySQL 8.4 Chapter 14,
organized by documentation section. Used to validate parser support.

---

## 14.5 Flow Control Functions

- `CASE`
- `IF()`
- `IFNULL()`
- `NULLIF()`

---

## 14.6 Numeric Functions

- `ABS()`
- `ACOS()`
- `ASIN()`
- `ATAN()`
- `ATAN2()`
- `CEIL()`
- `CEILING()`
- `CONV()`
- `COS()`
- `COT()`
- `CRC32()`
- `DEGREES()`
- `EXP()`
- `FLOOR()`
- `LN()`
- `LOG()`
- `LOG10()`
- `LOG2()`
- `MOD()`
- `PI()`
- `POW()`
- `POWER()`
- `RADIANS()`
- `RAND()`
- `ROUND()`
- `SIGN()`
- `SIN()`
- `SQRT()`
- `TAN()`
- `TRUNCATE()`

---

## 14.7 Date and Time Functions

- `ADDDATE()`
- `ADDTIME()`
- `CONVERT_TZ()`
- `CURDATE()`
- `CURRENT_DATE()`
- `CURRENT_TIME()`
- `CURRENT_TIMESTAMP()`
- `CURTIME()`
- `DATE()`
- `DATE_ADD()`
- `DATE_FORMAT()`
- `DATE_SUB()`
- `DATEDIFF()`
- `DAY()`
- `DAYNAME()`
- `DAYOFMONTH()`
- `DAYOFWEEK()`
- `DAYOFYEAR()`
- `EXTRACT()`
- `FROM_DAYS()`
- `FROM_UNIXTIME()`
- `GET_FORMAT()`
- `HOUR()`
- `LAST_DAY()`
- `LOCALTIME()`
- `LOCALTIMESTAMP()`
- `MAKEDATE()`
- `MAKETIME()`
- `MICROSECOND()`
- `MINUTE()`
- `MONTH()`
- `MONTHNAME()`
- `NOW()`
- `PERIOD_ADD()`
- `PERIOD_DIFF()`
- `QUARTER()`
- `SEC_TO_TIME()`
- `SECOND()`
- `STR_TO_DATE()`
- `SUBDATE()`
- `SUBTIME()`
- `SYSDATE()`
- `TIME()`
- `TIME_FORMAT()`
- `TIME_TO_SEC()`
- `TIMEDIFF()`
- `TIMESTAMP()`
- `TIMESTAMPADD()`
- `TIMESTAMPDIFF()`
- `TO_DAYS()`
- `TO_SECONDS()`
- `UNIX_TIMESTAMP()`
- `UTC_DATE()`
- `UTC_TIME()`
- `UTC_TIMESTAMP()`
- `WEEK()`
- `WEEKDAY()`
- `WEEKOFYEAR()`
- `YEAR()`
- `YEARWEEK()`

---

## 14.8 String Functions

- `ASCII()`
- `BIN()`
- `BIT_LENGTH()`
- `CHAR()`
- `CHAR_LENGTH()`
- `CHARACTER_LENGTH()`
- `CONCAT()`
- `CONCAT_WS()`
- `ELT()`
- `EXPORT_SET()`
- `FIELD()`
- `FIND_IN_SET()`
- `FORMAT()`
- `FROM_BASE64()`
- `HEX()`
- `INSERT()`
- `INSTR()`
- `LCASE()`
- `LEFT()`
- `LENGTH()`
- `LOAD_FILE()`
- `LOCATE()`
- `LOWER()`
- `LPAD()`
- `LTRIM()`
- `MAKE_SET()`
- `MID()`
- `OCT()`
- `OCTET_LENGTH()`
- `ORD()`
- `POSITION()`
- `QUOTE()`
- `REGEXP_INSTR()`
- `REGEXP_LIKE()`
- `REGEXP_REPLACE()`
- `REGEXP_SUBSTR()`
- `REPEAT()`
- `REPLACE()`
- `REVERSE()`
- `RIGHT()`
- `RPAD()`
- `RTRIM()`
- `SOUNDEX()`
- `SPACE()`
- `STRCMP()`
- `SUBSTR()`
- `SUBSTRING()`
- `SUBSTRING_INDEX()`
- `TO_BASE64()`
- `TRIM()`
- `UCASE()`
- `UNHEX()`
- `UPPER()`
- `WEIGHT_STRING()`

---

## 14.9 Full-Text Search Functions

- `MATCH() ... AGAINST()`

---

## 14.10 Cast Functions

- `CAST()`
- `CONVERT()`

---

## 14.11 XML Functions

- `ExtractValue()`
- `UpdateXML()`

---

## 14.12 Bit Functions

- `BIT_COUNT()`

*(Operators `&`, `|`, `^`, `~`, `<<`, `>>` excluded)*

---

## 14.13 Encryption and Compression Functions

- `AES_DECRYPT()`
- `AES_ENCRYPT()`
- `COMPRESS()`
- `MD5()`
- `RANDOM_BYTES()`
- `SHA()`
- `SHA1()`
- `SHA2()`
- `STATEMENT_DIGEST()`
- `STATEMENT_DIGEST_TEXT()`
- `UNCOMPRESS()`
- `UNCOMPRESSED_LENGTH()`
- `VALIDATE_PASSWORD_STRENGTH()`

---

## 14.14 Locking Functions

- `GET_LOCK()`
- `IS_FREE_LOCK()`
- `IS_USED_LOCK()`
- `RELEASE_ALL_LOCKS()`
- `RELEASE_LOCK()`

---

## 14.15 Information Functions

- `BENCHMARK()`
- `CHARSET()`
- `COERCIBILITY()`
- `COLLATION()`
- `CONNECTION_ID()`
- `CURRENT_ROLE()`
- `CURRENT_USER()`
- `DATABASE()`
- `FOUND_ROWS()`
- `ICU_VERSION()`
- `LAST_INSERT_ID()`
- `ROLES_GRAPHML()`
- `ROW_COUNT()`
- `SCHEMA()`
- `SESSION_USER()`
- `SYSTEM_USER()`
- `USER()`
- `VERSION()`

---

## 14.16 Spatial Analysis Functions

- `GeomCollection()`
- `GeometryCollection()`
- `LineString()`
- `MBRContains()`
- `MBRCoveredBy()`
- `MBRCovers()`
- `MBRDisjoint()`
- `MBREquals()`
- `MBRIntersects()`
- `MBROverlaps()`
- `MBRTouches()`
- `MBRWithin()`
- `MultiLineString()`
- `MultiPoint()`
- `MultiPolygon()`
- `Point()`
- `Polygon()`
- `ST_Area()`
- `ST_AsBinary()` / `ST_AsWKB()`
- `ST_AsGeoJSON()`
- `ST_AsText()` / `ST_AsWKT()`
- `ST_Buffer()`
- `ST_Buffer_Strategy()`
- `ST_Centroid()`
- `ST_Collect()`
- `ST_Contains()`
- `ST_ConvexHull()`
- `ST_Crosses()`
- `ST_Difference()`
- `ST_Dimension()`
- `ST_Disjoint()`
- `ST_Distance()`
- `ST_Distance_Sphere()`
- `ST_EndPoint()`
- `ST_Envelope()`
- `ST_Equals()`
- `ST_ExteriorRing()`
- `ST_FrechetDistance()`
- `ST_GeoHash()`
- `ST_GeomCollFromText()` / `ST_GeometryCollectionFromText()` / `ST_GeomCollFromTxt()`
- `ST_GeomCollFromWKB()` / `ST_GeometryCollectionFromWKB()`
- `ST_GeometryN()`
- `ST_GeometryType()`
- `ST_GeomFromGeoJSON()`
- `ST_GeomFromText()` / `ST_GeometryFromText()`
- `ST_GeomFromWKB()` / `ST_GeometryFromWKB()`
- `ST_HausdorffDistance()`
- `ST_InteriorRingN()`
- `ST_Intersection()`
- `ST_Intersects()`
- `ST_IsClosed()`
- `ST_IsEmpty()`
- `ST_IsSimple()`
- `ST_IsValid()`
- `ST_LatFromGeoHash()`
- `ST_Latitude()`
- `ST_Length()`
- `ST_LineFromText()` / `ST_LineStringFromText()`
- `ST_LineFromWKB()` / `ST_LineStringFromWKB()`
- `ST_LineInterpolatePoint()`
- `ST_LineInterpolatePoints()`
- `ST_LongFromGeoHash()`
- `ST_Longitude()`
- `ST_MakeEnvelope()`
- `ST_MLineFromText()` / `ST_MultiLineStringFromText()`
- `ST_MLineFromWKB()` / `ST_MultiLineStringFromWKB()`
- `ST_MPointFromText()` / `ST_MultiPointFromText()`
- `ST_MPointFromWKB()` / `ST_MultiPointFromWKB()`
- `ST_MPolyFromText()` / `ST_MultiPolygonFromText()`
- `ST_MPolyFromWKB()` / `ST_MultiPolygonFromWKB()`
- `ST_NumGeometries()`
- `ST_NumInteriorRing()` / `ST_NumInteriorRings()`
- `ST_NumPoints()`
- `ST_Overlaps()`
- `ST_PointAtDistance()`
- `ST_PointFromGeoHash()`
- `ST_PointFromText()`
- `ST_PointFromWKB()`
- `ST_PointN()`
- `ST_PolyFromText()` / `ST_PolygonFromText()`
- `ST_PolyFromWKB()` / `ST_PolygonFromWKB()`
- `ST_Simplify()`
- `ST_SRID()`
- `ST_StartPoint()`
- `ST_SwapXY()`
- `ST_SymDifference()`
- `ST_Touches()`
- `ST_Transform()`
- `ST_Union()`
- `ST_Validate()`
- `ST_Within()`
- `ST_X()`
- `ST_Y()`

---

## 14.17 JSON Functions

- `JSON_ARRAY()`
- `JSON_ARRAY_APPEND()`
- `JSON_ARRAY_INSERT()`
- `JSON_ARRAYAGG()` *(also an aggregate function)*
- `JSON_CONTAINS()`
- `JSON_CONTAINS_PATH()`
- `JSON_DEPTH()`
- `JSON_EXTRACT()`
- `JSON_INSERT()`
- `JSON_KEYS()`
- `JSON_LENGTH()`
- `JSON_MERGE()` *(deprecated, synonym for JSON_MERGE_PRESERVE)*
- `JSON_MERGE_PATCH()`
- `JSON_MERGE_PRESERVE()`
- `JSON_OBJECT()`
- `JSON_OBJECTAGG()` *(also an aggregate function)*
- `JSON_OVERLAPS()`
- `JSON_PRETTY()`
- `JSON_QUOTE()`
- `JSON_REMOVE()`
- `JSON_REPLACE()`
- `JSON_SCHEMA_VALID()`
- `JSON_SCHEMA_VALIDATION_REPORT()`
- `JSON_SEARCH()`
- `JSON_SET()`
- `JSON_STORAGE_FREE()`
- `JSON_STORAGE_SIZE()`
- `JSON_TABLE()`
- `JSON_TYPE()`
- `JSON_UNQUOTE()`
- `JSON_VALID()`
- `JSON_VALUE()`
- `MEMBER OF()`

*(Operators `->` and `->>` excluded)*

---

## 14.18 Replication Functions

- `asynchronous_connection_failover_add_managed()`
- `asynchronous_connection_failover_add_source()`
- `asynchronous_connection_failover_delete_managed()`
- `asynchronous_connection_failover_delete_source()`
- `asynchronous_connection_failover_reset()`
- `group_replication_disable_member_action()`
- `group_replication_enable_member_action()`
- `group_replication_get_communication_protocol()`
- `group_replication_get_write_concurrency()`
- `group_replication_reset_member_actions()`
- `group_replication_set_as_primary()`
- `group_replication_set_communication_protocol()`
- `group_replication_set_write_concurrency()`
- `group_replication_switch_to_multi_primary_mode()`
- `group_replication_switch_to_single_primary_mode()`
- `MASTER_POS_WAIT()` *(deprecated)*
- `SOURCE_POS_WAIT()`
- `WAIT_FOR_EXECUTED_GTID_SET()`

---

## 14.19 Aggregate Functions

- `AVG()`
- `BIT_AND()`
- `BIT_OR()`
- `BIT_XOR()`
- `COUNT()`
- `COUNT(DISTINCT)`
- `GROUP_CONCAT()`
- `JSON_ARRAYAGG()`
- `JSON_OBJECTAGG()`
- `MAX()`
- `MIN()`
- `STD()`
- `STDDEV()`
- `STDDEV_POP()`
- `STDDEV_SAMP()`
- `SUM()`
- `VAR_POP()`
- `VAR_SAMP()`
- `VARIANCE()`

---

## 14.20 Window Functions

*(Non-aggregate; most aggregate functions also work as window functions with OVER clause)*

- `CUME_DIST()`
- `DENSE_RANK()`
- `FIRST_VALUE()`
- `LAG()`
- `LAST_VALUE()`
- `LEAD()`
- `NTH_VALUE()`
- `NTILE()`
- `PERCENT_RANK()`
- `RANK()`
- `ROW_NUMBER()`

---

## 14.21 Performance Schema Functions

- `FORMAT_BYTES()`
- `FORMAT_PICO_TIME()`
- `PS_CURRENT_THREAD_ID()`
- `PS_THREAD_ID()`

---

## 14.22 Internal Functions

*(Internal use only — invoking as a user results in an error)*

- `CAN_ACCESS_COLUMN()`
- `CAN_ACCESS_DATABASE()`
- `CAN_ACCESS_TABLE()`
- `CAN_ACCESS_USER()`
- `CAN_ACCESS_VIEW()`
- `GET_DD_COLUMN_PRIVILEGES()`
- `GET_DD_CREATE_OPTIONS()`
- `GET_DD_INDEX_SUB_PART_LENGTH()`
- `INTERNAL_AUTO_INCREMENT()`
- `INTERNAL_AVG_ROW_LENGTH()`
- `INTERNAL_CHECK_TIME()`
- `INTERNAL_CHECKSUM()`
- `INTERNAL_DATA_FREE()`
- `INTERNAL_DATA_LENGTH()`
- `INTERNAL_DD_CHAR_LENGTH()`
- `INTERNAL_GET_COMMENT_OR_ERROR()`
- `INTERNAL_GET_ENABLED_ROLE_JSON()`
- `INTERNAL_GET_HOSTNAME()`
- `INTERNAL_GET_USERNAME()`
- `INTERNAL_GET_VIEW_WARNING_OR_ERROR()`
- `INTERNAL_INDEX_COLUMN_CARDINALITY()`
- `INTERNAL_INDEX_LENGTH()`
- `INTERNAL_IS_ENABLED_ROLE()`
- `INTERNAL_IS_MANDATORY_ROLE()`
- `INTERNAL_KEYS_DISABLED()`
- `INTERNAL_MAX_DATA_LENGTH()`
- `INTERNAL_TABLE_ROWS()`
- `INTERNAL_UPDATE_TIME()`
- `IS_VISIBLE_DD_OBJECT()`

---

## 14.23 Miscellaneous Functions

- `ANY_VALUE()`
- `BIN_TO_UUID()`
- `DEFAULT()`
- `GROUPING()`
- `INET_ATON()`
- `INET_NTOA()`
- `INET6_ATON()`
- `INET6_NTOA()`
- `IS_IPV4()`
- `IS_IPV4_COMPAT()`
- `IS_IPV4_MAPPED()`
- `IS_IPV6()`
- `IS_UUID()`
- `NAME_CONST()`
- `SLEEP()`
- `UUID()`
- `UUID_SHORT()`
- `UUID_TO_BIN()`
- `VALUES()`
