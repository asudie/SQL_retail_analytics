--- part 2
--- --- --- 2nd view Purchase history
---
DROP VIEW IF EXISTS Purchase_history_view;

CREATE VIEW Purchase_history_view AS
(
SELECT C.customer_id,
       T.transaction_id,
       transaction_datetime,
       Group_ID,
       SUM(St.SKU_purchase_price * Ch.SKU_amount) :: numeric AS group_cost,
       SUM(Ch.SKU_summ) :: numeric                           AS group_summ,
       SUM(Ch.SKU_summ_paid) :: numeric                      AS group_summ_paid
FROM cards C
         JOIN Transactions T ON C.customer_card_id = T.customer_card_id
         JOIN Checks Ch ON T.transaction_id = Ch.transaction_id
         JOIN Product_grid Pg ON Ch.SKU_id = Pg.SKU_id
         JOIN Stores St ON (
            Pg.SKU_id = St.SKU_id
        AND T.transaction_store_id = St.transaction_store_id
    )
GROUP BY C.customer_id,
         T.transaction_id,
         T.transaction_datetime,
         group_id
    );

--- TEST CASES
SELECT *
FROM purchase_history_view
WHERE customer_id = 1;

SELECT *
FROM purchase_history_view
WHERE group_summ_paid > 250;

SELECT customer_id,
       group_cost
FROM purchase_history_view
WHERE group_id = 1
  AND group_summ > 20;

SELECT *
FROM purchase_history_view
ORDER BY 1,
         4,
         3;
