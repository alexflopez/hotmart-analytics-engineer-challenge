-- Dados de exemplo utilizados para validação funcional da solução.
-- Representam o resultado esperado após aplicação das regras de negócio,


INSERT INTO gmv_purchase_snapshot
    (snapshot_date, purchase_id, order_date, release_date, purchase_status, purchase_value, subsidiary, transaction_date)
VALUES
    ('2023-01-23', 55, '2023-01-20', '2023-01-20', 'APROVADA', 55.00, 'nacional', '2023-01-20');
	
INSERT INTO gmv_purchase_snapshot
    (snapshot_date, purchase_id, order_date, release_date, purchase_status, purchase_value, subsidiary, transaction_date)
VALUES
    ('2023-02-05', 55, '2023-01-20', '2023-01-20', 'APROVADA', 55.00, 'nacional', '2023-01-20');


INSERT INTO gmv_purchase_snapshot
    (snapshot_date, purchase_id, order_date, release_date, purchase_status, purchase_value, subsidiary, transaction_date)
VALUES
    ('2023-02-26', 69, '2023-02-26', '2023-02-26', 'APROVADA', 2000.00, 'internacional', '2023-02-26');
	
INSERT INTO gmv_purchase_snapshot
    (snapshot_date, purchase_id, order_date, release_date, purchase_status, purchase_value, subsidiary, transaction_date)
VALUES
    ('2023-07-12', 69, '2023-02-26', '2023-02-26', 'APROVADA', 1800.00, 'internacional', '2023-02-26');
	
INSERT INTO gmv_purchase_snapshot
    (snapshot_date, purchase_id, order_date, release_date, purchase_status, purchase_value, subsidiary, transaction_date)
VALUES
    ('2023-07-15', 55, '2023-01-20', '2023-07-15', 'APROVADA', 55.00, 'nacional', '2023-01-20');
	
-- ================================================================
-- Resultado esperado das queries
-- ================================================================
--
-- Query 1 — GMV corrente (MAX snapshot_date por purchase_id):
--
--   transaction_date | subsidiary     | gmv     | purchase_count
--   2023-01-20       | nacional       |   55.00 | 1
--   2023-02-26       | internacional  | 1800.00 | 1
--
-- Query 2 — GMV de fevereiro como estava em 2023-02-26:
--
--   transaction_date | subsidiary     | gmv
--   2023-02-26       | internacional  | 2000.00   ← valor original
--
-- Query 2 — GMV de fevereiro visto após 2023-07-12:
--
--   transaction_date | subsidiary     | gmv
--   2023-02-26       | internacional  | 1800.00   ← após correção
--
-- Demonstra a navegação temporal: mesma transação, valores
-- diferentes dependendo do snapshot_date de referência.
