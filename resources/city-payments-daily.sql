-- Total
SELECT 
re."LAUFD" AS "Date",
'Total' AS "PaymentType",
SUM(re."RBETR") AS "DailyTotal"

FROM "SAPR3"."REGUH" re

WHERE re."MANDT"='100'
  AND re."ZBUKR"='COCT'
  --AND re."LAUFD">20200101
  AND re."XVORL"!='X'
  
GROUP BY re."LAUFD"

UNION

-- Investments
SELECT 
re."LAUFD" AS "Date",
'Investments' AS "PaymentType",
SUM(re."RBETR") AS "DailyTotal"

FROM "SAPR3"."REGUH" re

WHERE re."MANDT"='100'
  AND re."ZBUKR"='COCT'
  --AND re."LAUFD">20200101
  AND re."XVORL"!='X'
  AND REGEXP_LIKE(re."LAUFI", '^IN.*$')
  
GROUP BY re."LAUFD"

UNION

-- Payroll
SELECT 
re."LAUFD" AS "Date",
'Payrolls' AS "PaymentType",
SUM(re."RBETR") AS "DailyTotal"

FROM "SAPR3"."REGUH" re

WHERE re."MANDT"='100'
  AND re."ZBUKR"='COCT'
  --AND re."LAUFD">20200101
  AND re."XVORL"!='X'
  AND REGEXP_LIKE(re."LAUFI", '^.*P$')
  
GROUP BY re."LAUFD"

UNION

-- Refunds
SELECT 
re."LAUFD" AS "Date",
'Refunds' AS "PaymentType",
SUM(re."RBETR") AS "DailyTotal"

FROM "SAPR3"."REGUH" re

WHERE re."MANDT"='100'
  AND re."ZBUKR"='COCT'
  --AND re."LAUFD">20200101
  AND re."XVORL"!='X'
  AND NOT (REGEXP_LIKE(re."LAUFI", '^.*P$') OR REGEXP_LIKE(re."LAUFI", '^IN.*$'))
  AND re."LIFNR"=' '
  
GROUP BY re."LAUFD"

UNION

-- Vendor
SELECT 
re."LAUFD" AS "Date",
'Vendors' AS "PaymentType",
SUM(re."RBETR") AS "DailyTotal"

FROM "SAPR3"."REGUH" re

WHERE re."MANDT"='100'
  AND re."ZBUKR"='COCT'
  --AND re."LAUFD">20200101
  AND re."XVORL"!='X'
  AND NOT (REGEXP_LIKE(re."LAUFI", '^.*P$') OR REGEXP_LIKE(re."LAUFI", '^IN.*$'))
  AND re."LIFNR"!=' '
  
GROUP BY re."LAUFD"

ORDER BY 1
;