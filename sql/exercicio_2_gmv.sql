
-- Estado mais recente de cada compra
WITH latest_snapshot AS (
    SELECT
        purchase_id,
        order_date,
        subsidiary,
        purchase_value,
        snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY purchase_id
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM gmv_purchase_snapshot
)
SELECT
    order_date                        AS transaction_date,
    subsidiary,
    ROUND(SUM(purchase_value), 2)     AS gmv,
    COUNT(purchase_id)                AS purchase_count,
    MAX(snapshot_date)                AS last_processed_at
FROM latest_snapshot
WHERE rn = 1
GROUP BY
    order_date,
    subsidiary
ORDER BY
    order_date,
    subsidiary;
	
-- Estado histórico de uma compra

WITH snapshot_at_date AS (
    SELECT
        purchase_id,
        order_date,
        subsidiary,
        purchase_value,
        snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY purchase_id
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM gmv_purchase_snapshot
    WHERE snapshot_date <= '2023-01-31'   -- <-- data de referência (Exemplo: mês de janeiro fechado
      AND transaction_date >= '2023-01-01'
      AND transaction_date <= '2023-01-31'
)
SELECT
    order_date                        AS transaction_date,
    subsidiary,
    ROUND(SUM(purchase_value), 2)     AS gmv,
    COUNT(purchase_id)                AS purchase_count
FROM snapshot_at_date
WHERE rn = 1
GROUP BY
    order_date,
    subsidiary
ORDER BY
    order_date,
    subsidiary;
	



