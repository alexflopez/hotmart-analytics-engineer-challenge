-- Consulta da visão corrente do GMV diário por subsidiária.
-- Como a solução utiliza snapshots imutáveis, os registros históricos
-- nunca são atualizados ou removidos.
-- A utilização de MAX(snapshot_date) retorna a versão mais recente
-- disponível dos dados, preservando a rastreabilidade temporal.

SELECT
    transaction_date,
    subsidiary,
    gmv,
    purchase_count,
    snapshot_date   AS processed_at
FROM gmv_daily_snapshot
WHERE snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM gmv_daily_snapshot
)
ORDER BY
    transaction_date,
    subsidiary;
