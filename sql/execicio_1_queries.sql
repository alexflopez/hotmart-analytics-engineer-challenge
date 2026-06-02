/*


*/


-- Exercício 1 
-- Quais são os 50 maiores produtores em faturamento ($) de 2021?


SELECT
    p.producer_id,
    SUM(pi.purchase_value)  AS total_revenue
FROM purchase p
INNER JOIN product_item pi
    ON  p.prod_item_id          = pi.prod_item_id
    AND p.prod_item_partition   = pi.prod_item_partition
WHERE
    p.release_date IS NOT NULL -- release_date precisa considerar somente o preenchimento como válido para este fim
    AND EXTRACT(YEAR FROM p.release_date) = 2021
GROUP BY
    p.producer_id
ORDER BY
    total_revenue DESC
LIMIT 50;
 
 
-- Exercício 1 
-- 2. Quais são os 2 produtos que mais faturaram ($) de cada produtor?

WITH revenue_by_producer_product AS (
    SELECT
        p.producer_id,
        pi.product_id,
        SUM(pi.purchase_value)  AS total_revenue
    FROM purchase p
    INNER JOIN product_item pi
        ON  p.prod_item_id          = pi.prod_item_id
        AND p.prod_item_partition   = pi.prod_item_partition
    WHERE
        p.release_date IS NOT NULL
    GROUP BY
        p.producer_id,
        pi.product_id
),
ranked AS (
    SELECT
        producer_id,
        product_id,
        total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY producer_id
            ORDER BY total_revenue DESC
        ) AS rank -- para este fim, precisamos pariticionar por producer_id (identificador do produtor) baseado na receita total (total) revenue.
    FROM revenue_by_producer_product
)
SELECT
    producer_id,
    product_id,
    total_revenue
FROM ranked
WHERE rank <= 2
ORDER BY
    producer_id,
    rank;
