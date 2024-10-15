-----------------------------
--- BASE DE CLIENTES 
-----------------------------
SELECT *
FROM  proceso_canales.clientes;
-----------------------------
--- BASE DE CANALES
-----------------------------
SELECT *
FROM  proceso_canales.canales;

-----------------------------
--- BASE DE TRANSACCIONES
-----------------------------
SELECT *
FROM  proceso_canales.transacciones;

-------------------------------------------------------
-- LIMPIEZA Y CREACION DE LA NUEVA TABLA DE CLIENTES 
---------------------------------------------------------
DROP TABLE PROCESO_CANALES.crsalced_data_clientes PURGE;
CREATE TABLE PROCESO_CANALES.crsalced_data_clientes STORED AS PARQUET AS 
WITH
S0 AS (
SELECT  tipo_doc,
        num_doc,
        nombre,
        tipo_persona,
        ingresos_mensuales,
        COUNT(*) OVER(PARTITION BY num_doc ORDER BY ingresos_mensuales DESC ) AS RN
FROM proceso_canales.clientes)
SELECT tipo_doc,
        num_doc,
        nombre,
        tipo_persona,
        ingresos_mensuales
FROM S0
WHERE  RN = 1;


-----------------------------------------------------------
-- UNION DE LAS TABLAS TRANSACIONES, CLIENTES Y CANALES
----------------------------------------------------------
DROP TABLE proceso_canales.crsalced_union_bases PURGE;
CREATE TABLE proceso_canales.crsalced_union_bases STORED AS PARQUET AS 
SELECT   T1.fecha_transaccion,
         T1.cod_canal,
         T1.tipo_doc,
         T1.num_doc,
         T1.naturaleza,
         T1.monto,
         CASE WHEN T2.tipo_persona = 'NATURAL' THEN 'PERSONA NATURAL' ELSE T2.tipo_persona END AS tipo_persona,
         T2.ingresos_mensuales,
         T3.tipo, 
         T3.cod_jurisdiccion
FROM proceso_canales.transacciones AS T1
INNER JOIN PROCESO_CANALES.crsalced_data_clientes AS T2
ON T1.num_doc = T2.num_doc
LEFT JOIN proceso_canales.canales AS T3
ON T1.cod_canal = T3.codigo ;


--------------------
--REPORTE-----------
--------------------
WITH 
S0 AS (
        SELECT tipo_persona,
               num_doc,
               ingresos_mensuales,
               GROUP_CONCAT(DISTINCT TIPO," | ") AS canales_usados,
               SUM(monto) AS Suma_monto
        FROM proceso_canales.crsalced_union_bases
        WHERE fecha_transaccion >= ADD_months (NOW(),-6)
        GROUP BY 1,2,3),
S1 AS (
            SELECT tipo_persona,
                   num_doc, 
                   ingresos_mensuales,
                   canales_usados,
                   Suma_monto,
                   NTILE(100) OVER (PARTITION BY tipo_persona ORDER BY ingresos_mensuales) AS percentil
           FROM S0
           WHERE suma_monto >= (ingresos_mensuales * 2))
SELECT  tipo_persona,
        num_doc, 
        suma_monto, 
        ingresos_mensuales,
        canales_usados
FROM S1
WHERE percentil > 95;


---------------------------------------------------------------------
-- UNION DEL MODELO ANTERIOR CON LA INFORMACION DEL GEO-PORTAL DANE
--------------------------------------------------------------------
DROP TABLE PROCESO_CANALES.CRSALCED_BASE_FINAL PURGE;
CREATE TABLE PROCESO_CANALES.CRSALCED_BASE_FINAL STORED AS PARQUET AS ;
WITH
UBI AS (
        SELECT *
        FROM  proceso_canales.base_municipios_col)
SELECT *
FROM proceso_canales.crsalced_union_bases   T1
LEFT JOIN UBI T2
ON CAST(T1.cod_jurisdiccion AS BIGINT) = CAST(T2.codigo AS BIGINT);


