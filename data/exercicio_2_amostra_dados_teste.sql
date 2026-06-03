-- Dados de exemplo utilizados para validação funcional da solução.
-- Representam o resultado esperado após aplicação das regras de negócio,
-- deduplicação dos eventos CDC e cálculo do GMV diário por subsidiária.
-- Os registros simulam a materialização de um snapshot histórico em 2023-07-16.

INSERT INTO gmv_daily_snapshot
    (snapshot_date, subsidiary, gmv, purchase_count, transaction_date)
VALUES
    ('2023-07-16', 'nacional',       55.00, 1, '2023-01-20'),
    ('2023-07-16', 'internacional', 2000.00, 1, '2023-02-26');
