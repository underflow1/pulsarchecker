	DROP TABLE IF EXISTS data_child_tmp;
	DROP TABLE IF EXISTS data_parent_tmp;
	DROP TABLE IF EXISTS arhiv_tmp;
	DROP TABLE IF EXISTS date_range_tmp;
	
	CREATE TEMPORARY TABLE data_child_tmp
		(
		"F0" timestamp without time zone,
		"F2" double precision
		);
	
	CREATE TEMPORARY TABLE data_parent_tmp
		(
		"F0" timestamp without time zone,
		"F1" double precision
		);
	
	CREATE TEMPORARY TABLE arhiv_tmp
		(
		"DataValue" double precision,
		typ_arh integer,
		pr_id integer,
		"DateValue" timestamp without time zone,
		tp_dat integer,
		"Delta" double precision
		);

	CREATE TEMPORARY TABLE date_range_tmp
		(
		"DateValue" timestamp without time zone
		);

	INSERT INTO date_range_tmp("DateValue")
		SELECT CASE
			WHEN $type_a = 1 THEN 
			"Tepl"."GetDateRange"($date_s, $date_e, '1 hour')
			WHEN $type_a = 2 THEN 
			"Tepl"."GetDateRange"($date_s, $date_e, '1 day')
			WHEN $type_a = 3 THEN 
			"Tepl"."GetDateRange"($date_s, $date_e, '1 month')
			END;

-- подгружаем архивные данные для всех приборов:
INSERT INTO arhiv_tmp(pr_id, typ_arh, "DateValue", "DataValue", "Delta")
	SELECT DISTINCT ON (pr_id, typ_arh, "DateValue") pr_id, typ_arh, "DateValue", "DataValue", "Delta" FROM "Tepl"."Arhiv_cnt"
	WHERE
	pr_id IN
	(
		SELECT prp_id FROM "Tepl"."ParamResPlc_cnt" p
		WHERE plc_id IN (SELECT plc_id FROM "Tepl"."Places_cnt" WHERE (place_id = $place or plc_id = $place)
		AND "ParamRes_id" IN (1))
		AND typ_arh = $type_a
-- указываем смещение начальной даты, для того чтобы в отчете не было пустой первой строки:
		AND CASE
			WHEN $type_a = 1 THEN 
				"DateValue" BETWEEN timestamp $date_s - interval '1 hour'  AND timestamp $date_e
			WHEN $type_a = 2 THEN 
				"DateValue" BETWEEN timestamp $date_s - interval '1 day'  AND timestamp $date_e
			WHEN $type_a = 3 THEN 
				"DateValue" BETWEEN timestamp $date_s - interval '1 month'  AND timestamp $date_e
			END
	);


INSERT INTO data_child_tmp("F0", "F2")
SELECT
	results.*
	FROM
	(
	SELECT
	dts.date as "F0", -- выбираем даты из таблицы-столбца дат
	CAST(SUM(a1."Delta") as double precision) as "F2" 
	--CAST(SUM(1) as double precision) as "F2" 
	--CAST(SUM(a1."DataValue" - a_prev1."DataValue") as double precision) as "F2"

	FROM "Tepl"."Places_cnt" p
	INNER JOIN "Tepl"."PlaceTyp_cnt" pt ON p.typ_id = pt.typ_id
	INNER JOIN date_range_tmp dts(date) ON 1=1
	LEFT JOIN "Tepl"."ParamResPlc_cnt" prp1 ON p.plc_id = prp1.plc_id AND prp1."ParamRes_id" = 1
	LEFT JOIN arhiv_tmp a1 ON prp1.prp_id = a1.pr_id AND a1.typ_arh = $type_a 
	AND a1."DateValue" = dts.date
	
	LEFT JOIN "Tepl"."ParamResPlc_cnt" prp_prev1 ON p.plc_id = prp_prev1.plc_id AND prp_prev1."ParamRes_id" = 1 
	
	WHERE p.place_id = $place
	GROUP BY "F0"
	ORDER BY "F0"
	) results;

INSERT INTO data_parent_tmp("F0", "F1")
SELECT
	results.*
	FROM
	(
	SELECT
	dts.date as "F0",
	a1."Delta" as  "F1" 
	FROM "Tepl"."Places_cnt" p 
	INNER JOIN date_range_tmp dts(date) ON 1=1 
	LEFT JOIN "Tepl"."ParamResPlc_cnt" prp_prev1 ON p.plc_id = prp_prev1.plc_id AND prp_prev1."ParamRes_id" = 1 
	LEFT JOIN "Tepl"."ParamResPlc_cnt" prp1 ON p.plc_id = prp1.plc_id AND prp1."ParamRes_id" = 1
	LEFT JOIN arhiv_tmp a1 ON prp1.prp_id = a1.pr_id AND a1.typ_arh = $type_a 
	AND a1."DateValue" = dts.date

	WHERE p.plc_id = $place
	ORDER BY "F0"
	) results ;


	SELECT dp."F0", dp."F1" as "F5" , dc."F2" as "F10", dc."F2" - dp."F1" AS "F15", ((dc."F2" - dp."F1") / dp."F1" * 100) AS "F20"

	FROM  data_parent_tmp dp
	INNER JOIN data_child_tmp dc ON dp."F0" = dc."F0"
	WHERE @(@(dp."F1")  - @(dc."F2")) > 0.05 *  @(dp."F1")
