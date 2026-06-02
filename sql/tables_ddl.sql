# Dialeto utilizado: ANSI SQL

CREATE TABLE product_item (
    prod_item_id            BIGINT      NOT NULL,   -- identificador do item de compra
    prod_item_partition     BIGINT,                 -- partição no lake para o item da compra
    product_id              BIGINT      NOT NULL,   -- identificador do produto
    item_quantity           INTEGER     NOT NULL,   -- quantidade comprada por item
    purchase_value          FLOAT,                  -- valor do item de compra
    transaction_datetime    TIMESTAMP   NOT NULL,   -- momento de inserção do dado no lake
    transaction_date        DATE        NOT NULL,   -- data de inserção do dado no lake
 
    CONSTRAINT pk_product_item PRIMARY KEY (prod_item_id)
);
 
-- -------------------------------------------------------------
 
CREATE TABLE purchase (
    purchase_id             BIGINT      NOT NULL,   -- identificador da compra
    buyer_id                BIGINT      NOT NULL,   -- identificador do comprador
    prod_item_id            BIGINT      NOT NULL,   -- identificador do item de compra
    order_date              DATE        NOT NULL,   -- data do pedido de compra
    release_date            DATE,                   -- data de liberação da compra mediante a confirmação do pagamento
    producer_id             BIGINT      NOT NULL,   -- identificador do produtor
    purchase_partition      BIGINT,                 -- partição no lake para a compra
    prod_item_partition     BIGINT,                 -- partição no lake para o item de compra
    purchase_total_value    FLOAT,                  -- valor total da compra
    purchase_status         VARCHAR(20),            -- status da compra: INICIADA, APROVADA, CANCELADA, REEMBOLSADA
    transaction_datetime    TIMESTAMP   NOT NULL,   -- momento de inserção do dado no lake
    transaction_date        DATE        NOT NULL,   -- data de inserção do dado no lake
 
    CONSTRAINT pk_purchase PRIMARY KEY (purchase_id),
    CONSTRAINT fk_purchase_product_item
        FOREIGN KEY (prod_item_id)
        REFERENCES product_item (prod_item_id)
);
 
-- -------------------------------------------------------------
 
CREATE TABLE order_transaction_cost_hist (
    purchase_id                              BIGINT  NOT NULL,   -- identificador da compra
    purchase_partition                       BIGINT,             -- partição no lake para a compra
    order_transaction_cost_vat_value         FLOAT,              -- valor VAT referente a compra
    order_transaction_cost_installment_value FLOAT,              -- valor do parcelamento da compra
    order_transaction_cost_date              DATE,               -- data da efetivação do parcelamento
    transaction_datetime                     TIMESTAMP NOT NULL, -- momento de inserção do dado no lake
    transaction_date                         DATE      NOT NULL, -- data de inserção do dado no lake
 
    CONSTRAINT fk_order_transaction_cost_hist_purchase
        FOREIGN KEY (purchase_id)
        REFERENCES purchase (purchase_id)
);
 
-- -------------------------------------------------------------
 
CREATE TABLE purchase_extra_info (
    purchase_id             BIGINT      NOT NULL,   -- identificador da compra
    purchase_partition      BIGINT,                 -- partição no lake para a compra
    subsidiary              VARCHAR(100),           -- empresa que, embora controlada (dirigida) por outra, possui grande parte ou o total de suas ações
    transaction_datetime    TIMESTAMP   NOT NULL,   -- momento de inserção do dado no lake
    transaction_date        DATE        NOT NULL,   -- data de inserção do dado no lake
 
    CONSTRAINT fk_purchase_extra_info_purchase
        FOREIGN KEY (purchase_id)
        REFERENCES purchase (purchase_id)
);
 