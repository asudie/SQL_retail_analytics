--- part 2
--- --- --- 3rd view Periods
---
DROP VIEW IF EXISTS Periods_view;

CREATE VIEW Periods_view AS
(
WITH Support AS (SELECT C.Customer_id,
                        T.transaction_id,
                        T.transaction_datetime,
                        Group_id,
                        SKU_discount,
                        SKU_summ
                 FROM cards C
                          JOIN Transactions T ON C.customer_card_id = T.customer_card_id
                          JOIN Checks Ch ON T.transaction_id = Ch.transaction_id
                          JOIN Product_grid Pg ON Ch.SKU_id = Pg.SKU_id)
SELECT Customer_id,
       Group_ID,
       MIN(transaction_datetime)                         AS First_group_purchase_date,
       MAX(transaction_datetime)                         AS Last_group_purchase_date,
       COUNT(DISTINCT transaction_id) :: numeric         AS Group_purchase,
       (
                   EXTRACT(
                           EPOCH
                           FROM
                           MAX(transaction_datetime) - MIN(transaction_datetime)
                       ) / 60 / 60 / 24 + 1
           ) / COUNT(DISTINCT transaction_id) :: numeric AS Group_frequency,
       COALESCE(
               MIN(
                       CASE
                           WHEN SKU_discount > 0 THEN SKU_discount / SKU_summ
                           END
                   ),
               0
           )                                             AS Group_min_discount
FROM Support
GROUP BY customer_id,
         group_id
    );

--- TEST CASES
SELECT *
FROM Periods_view
ORDER BY 1,
         3,
         2;

SELECT *
FROM Periods_view
WHERE group_id = 7
ORDER BY 1;

SELECT *
FROM Periods_view
WHERE group_min_discount > 0.1
ORDER BY 1;

SELECT *
FROM Periods_view
WHERE customer_id = 3;
