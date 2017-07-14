# CalciteUnionError

## Attempt to union using 

### RelBuilder
```java
builder.pushAll(Arrays.asList(first, second))
        .union(true);
String generatedSql = toSql(builder.build(), connection);
```

### Manual
```java
String manualSql = "(" + toSql(second, connection) + ")";
manualSql += "\nUNION ALL\n";
manualSql += "(" + toSql(first, connection) + ")";
```

## The tests

### Without `ORDER BY` AND `LIMIT`

[SUCCESS] Manual:
```sql
(SELECT * 
FROM "PUBLIC"."TEST")

UNION ALL

(SELECT * 
FROM "PUBLIC"."TEST")
```

[SUCCESS] Using `RelBuilder`:
```sql
SELECT * FROM (
  SELECT * FROM "PUBLIC"."TEST"
  
  UNION ALL
  
  SELECT * FROM "PUBLIC"."TEST"
)
```

### With `ORDER BY` AND `LIMIT`

[SUCCESS] Manual:
```sql
(SELECT *
FROM "PUBLIC"."TEST"
ORDER BY "TEST_VALUE" NULLS LAST
OFFSET 10 ROWS
FETCH NEXT 20 ROWS ONLY)

UNION ALL

(SELECT *
FROM "PUBLIC"."TEST"
ORDER BY "TEST_VALUE" NULLS LAST
FETCH NEXT 10 ROWS ONLY)
```

[FAILED]  Using `RelBuilder`:
```sql
SELECT * FROM (
    SELECT *
    FROM "PUBLIC"."TEST"
    ORDER BY "TEST_VALUE" NULLS LAST
    FETCH NEXT 10 ROWS ONLY
    
    UNION ALL

    SELECT *
    FROM "PUBLIC"."TEST"
    ORDER BY "TEST_VALUE" NULLS LAST
    OFFSET 10 ROWS
    FETCH NEXT 20 ROWS ONLY
)
```