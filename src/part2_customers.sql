--- part 2
--- --- --- 1st view Customers
---
DROP VIEW IF EXISTS Customers_view;

CREATE VIEW Customers_view AS
WITH Customer_transactions AS (SELECT Customer_id,
                                      Transaction_summ,
                                      Transaction_datetime,
                                      Transaction_store_id
                               FROM cards C
                                        JOIN transactions TR ON C.Customer_card_id = TR.Customer_card_id),
     Average_checks AS (SELECT Customer_ID,
                               Round(AVG(Transaction_Summ), 2) AS Customer_average_check --2
                        FROM Customer_transactions
                        GROUP BY Customer_ID
                        ORDER BY 1),
     Average_checks_segments AS (SELECT Customer_id,
                                        CASE
                                            WHEN Percent_range < 0.1 THEN 'High'
                                            WHEN Percent_range < 0.35 THEN 'Medium'
                                            ELSE 'Low'
                                            END AS Customer_average_check_segment --3
                                 FROM (SELECT Customer_ID,
                                              PERCENT_RANK() OVER (
                                                  ORDER BY
                                                      Customer_average_check DESC
                                                  ) AS Percent_range
                                       FROM Average_checks) AS Customers_ranked),
     Customers_frequency AS (SELECT Customer_ID,
                                    Round(
                                                EXTRACT(
                                                        EPOCH
                                                        FROM
                                                        (
                                                            MAX(Transaction_datetime) - MIN(Transaction_datetime)
                                                            )
                                                    ) / 60 / 60 / 24 / COUNT(*),
                                                2
                                        ) AS Customer_frequency --4
                             FROM Customer_transactions
                             GROUP BY Customer_ID),
     Customers_frequency_segment AS (SELECT Customer_id,
                                            CASE
                                                WHEN Percent_range < 0.1 THEN 'Often'
                                                WHEN Percent_range < 0.35 THEN 'Occasionally'
                                                ELSE 'Rarely'
                                                END AS Customer_frequency_segment --5
                                     FROM (SELECT Customer_ID,
                                                  PERCENT_RANK() OVER (
                                                      ORDER BY
                                                          Customer_frequency
                                                      ) AS Percent_range
                                           FROM Customers_frequency) AS Transactions_ranked),
     Customers_incative_period AS (SELECT Customer_ID,
                                          Round(
                                                      EXTRACT(
                                                              EPOCH
                                                              FROM
                                                              (
                                                                      (SELECT analysis_formation
                                                                       FROM date_of_analysis_formation) -
                                                                      MAX(Transaction_datetime)
                                                                  )
                                                          ) / 60 / 60 / 24,
                                                      2
                                              ) AS Customer_inactive_period --6
                                   FROM Customer_transactions
                                   GROUP BY Customer_ID),
     Customers_churn_rate AS (SELECT CI.Customer_ID,
                                     CI.Customer_inactive_period / CF.Customer_frequency AS Customer_churn_rate --7
                              FROM Customers_frequency CF
                                       JOIN Customers_incative_period CI ON CF.Customer_ID = CI.Customer_ID),
     Customers_churn_rate_segment AS (SELECT Customer_id,
                                             CASE
                                                 WHEN Customer_churn_rate < 2.0 THEN 'Low'
                                                 WHEN Customer_churn_rate < 5.0 THEN 'Medium'
                                                 ELSE 'High'
                                                 END AS Customer_churn_segment --8
                                      FROM Customers_churn_rate),
     Customers_segments AS (SELECT A.Customer_ID,
                                   SEGMENT AS Customer_segment --9
                            FROM Average_checks_segments A
                                     JOIN Customers_frequency_segment F ON A.Customer_ID = F.Customer_ID
                                     JOIN Customers_churn_rate_segment C ON F.Customer_ID = C.Customer_ID
                                     LEFT JOIN Segments S ON S.Average_Check = A.Customer_average_check_segment
                                AND S.Frequency_of_purchases = F.Customer_frequency_segment
                                AND S.Churn_probability = C.Customer_churn_segment),
     Customers_stores_transactions AS (SELECT customer_id,
                                              transaction_store_id,
                                              MAX(transaction_datetime)                                AS last_transaction,
                                              COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY customer_id) AS part_of_transactions
                                       FROM Customer_transactions
                                       GROUP BY customer_id,
                                                transaction_store_id),
     Customers AS (SELECT customer_id
                   FROM Customer_transactions
                   GROUP BY customer_id),
     Customers_3_last_same_transactions AS (SELECT *
                                            FROM (SELECT customer_id,
                                                         transaction_store_id
                                                  FROM (SELECT customer_id,
                                                               transaction_store_id,
                                                               RANK() OVER (
                                                                   PARTITION BY customer_id
                                                                   ORDER BY
                                                                       transaction_datetime DESC
                                                                   ) AS last_visit
                                                        FROM Customer_transactions) AS last_stores
                                                  WHERE last_visit <= 3) AS three_last_stores
                                            GROUP BY customer_id,
                                                     transaction_store_id
                                            HAVING COUNT(*) = 3),
     Customers_primary_store AS (SELECT customer_id,
                                        CASE
                                            WHEN customer_id IN (SELECT customer_id
                                                                 FROM Customers_3_last_same_transactions LS)
                                                THEN (SELECT transaction_store_id
                                                      FROM Customers_3_last_same_transactions LS
                                                      WHERE LS.customer_id = Customers.customer_id)
                                            ELSE (SELECT transaction_store_id
                                                  FROM Customers_stores_transactions CST
                                                  WHERE CST.customer_id = customers.customer_id
                                                  ORDER BY part_of_transactions DESC,
                                                           last_transaction DESC
                                                  LIMIT 1)
                                            END AS Customer_primary_store --10
                                 FROM Customers)
SELECT C.Customer_ID,
       Customer_average_check,
       Customer_average_check_segment,
       Customer_frequency,
       Customer_frequency_segment,
       Customer_inactive_period,
       Customer_churn_rate,
       Customer_churn_segment,
       Customer_segment,
       Customer_primary_store
FROM customers C
         JOIN Average_checks AC ON C.Customer_id = AC.Customer_id
         JOIN Average_checks_segments ACS ON C.Customer_id = ACS.Customer_id
         JOIN Customers_frequency CF ON C.Customer_id = CF.Customer_id
         JOIN Customers_frequency_segment CFS ON C.Customer_id = CFS.Customer_id
         JOIN Customers_incative_period CIP ON C.Customer_id = CIP.Customer_id
         JOIN Customers_churn_rate CR ON C.Customer_id = CR.Customer_id
         JOIN Customers_churn_rate_segment CRS ON C.Customer_id = CRS.Customer_id
         JOIN Customers_segments CS ON C.Customer_id = CS.Customer_id
         JOIN Customers_primary_store PS ON C.Customer_id = PS.Customer_id
ORDER BY 1;

--- TEST CASES
SELECT *
FROM Customers_view
ORDER BY 1;

SELECT Customer_ID
FROM Customers_view
WHERE Customer_primary_store = 3
ORDER BY 1;

SELECT Customer_ID,
       Customer_segment
FROM Customers_view
WHERE Customer_inactive_period < 15
ORDER BY 1;
