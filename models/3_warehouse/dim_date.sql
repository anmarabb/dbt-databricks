SELECT
  date_format(CAST(d AS TIMESTAMP), 'yyyy-MM-dd') as id,
  d AS dim_date,
  year(d) AS year,
  weekofyear(d) AS year_week,
  dayofyear(d) AS year_day,
  year(d) AS fiscal_year,
  quarter(d) as fiscal_qtr,
  month(d) AS month,
  date_format(d, 'MMMM') as month_name,
  date_format(d, 'EEEE') AS week_day,
  date_format(d, 'EEEE') AS day_name,
  (CASE WHEN date_format(d, 'EEEE') IN ('Sunday', 'Saturday') THEN 0 ELSE 1 END) AS day_is_weekday
FROM (
  SELECT
    explode(sequence(to_date('2014-01-01'), to_date('2050-01-01'), interval 1 day)) AS d
)
