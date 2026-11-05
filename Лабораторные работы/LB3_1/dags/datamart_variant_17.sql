CREATE OR REPLACE VIEW titanic_datamart AS
SELECT row_id, passenger_id, p_class, sex, age,
 CASE WHEN age IS NULL THEN '6. Unknown'
      WHEN age < 13 THEN '1. 0–12'
      WHEN age < 18 THEN '2. 13–17'
      WHEN age < 35 THEN '3. 18–34'
      WHEN age < 60 THEN '4. 35–59'
      ELSE '5. 60+' END AS age_group,
 survived, p_class::TEXT AS class_label
FROM stg_titanic;
