WITH
  initial_facts AS (
  SELECT
    mr.id_review AS id_review,
    DATE_FROM_UNIX_DATE(lr.log_date) AS log_date,
    lr.device AS device,
    lr.location AS location,
    lr.os AS os,
    mr.positive_review AS Positive_review,
    up.quantity AS quantity,
    up.unit_price AS unit_price
  FROM
    `{{params.project_id}}.{{params.dataset_name}}.log_reviews` AS lr
  JOIN
    `{{params.project_id}}.{{params.dataset_name}}.movies_reviews` AS mr
  ON
    lr.id_review = mr.id_review
  JOIN
    `{{params.project_id}}.{{params.dataset_name}}.user_purchase` AS up
  ON
    up.customer_id = mr.cid )

SELECT
  dim_date.id_dim_date AS id_dim_date,
  dim_devices.id_dim_device AS id_dim_device,
  dim_location.id_dim_location AS id_dim_location,
  dim_os.id_dim_os AS id_dim_os,
  SUM(facts.quantity * facts.unit_price) AS amount_spent,
  SUM(facts.positive_review) AS review_score,
  COUNT(facts.id_review) AS review_count
FROM
  initial_facts AS facts
JOIN
  `{{params.project_id}}.{{params.dataset_name}}.dim_date` AS dim_date
ON
  facts.log_date = dim_date.log_date
JOIN
  `{{params.project_id}}.{{params.dataset_name}}.dim_os` AS dim_os
ON
  facts.os = dim_os.os
JOIN
  `{{params.project_id}}.{{params.dataset_name}}.dim_location` AS dim_location
ON
  facts.location = dim_location.location
JOIN
  `{{params.project_id}}.{{params.dataset_name}}.dim_devices` AS dim_devices
ON
  facts.device = dim_devices.device
GROUP BY
  dim_date.id_dim_date,
  dim_location.id_dim_location,
  dim_devices.id_dim_device,
  dim_os.id_dim_os ;
