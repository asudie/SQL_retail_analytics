--- part 2
--- --- --- 4th view Groups
---
DROP VIEW IF EXISTS Groups_view;

DROP FUNCTION IF EXISTS fnc_groups_view;

CREATE
    OR replace FUNCTION fnc_groups_view(
    Calc_method INT DEFAULT 1,
    Calc_period INT DEFAULT 3000
)
    returns TABLE
            (
                "Customer_id"            INTEGER,
                "Group_id"               INTEGER,
                "Group_affinity_index"   numeric,
                "Group_churn_rate"       numeric,
                "Group_stability_index"  numeric,
                "Group_margin"           numeric,
                "Group_discount_share"   numeric,
                "Group_minimum_discount" numeric,
                "Group_average_discount" numeric
            )
    LANGUAGE plpgsql
AS
$$
DECLARE
    date_analysis DATE := (SELECT Analysis_Formation :: DATE
                           FROM date_of_analysis_formation);

BEGIN
    IF
                Calc_method IN (1, 2)
            AND Calc_period > 0 THEN
        RETURN query WITH First_last_interval AS (SELECT DISTINCT Customer_id,
                                                                  First_group_purchase_date,
                                                                  Last_group_purchase_date
                                                  FROM periods_view
                                                  ORDER BY 1,
                                                           2),
                          transactions_date AS (SELECT C.Customer_id,
                                                       T.transaction_id,
                                                       T.transaction_datetime,
                                                       ROW_NUMBER() over (
                                                           ORDER BY
                                                               T.transaction_datetime DESC
                                                           ) AS Number_transactions
                                                FROM Cards C
                                                         JOIN Transactions T ON C.Customer_card_id = T.Customer_card_id),
                          Count_all_transactions AS (SELECT DISTINCT I.Customer_id,
                                                                     I.First_group_purchase_date,
                                                                     I.Last_group_purchase_date,
                                                                     COUNT(transaction_id) :: numeric AS Count_transactions
                                                     FROM transactions_date Td
                                                              JOIN First_last_interval I ON Td.Customer_id = I.Customer_id
                                                     WHERE Td.transaction_datetime >= I.First_group_purchase_date
                                                       AND Td.transaction_datetime <= I.Last_group_purchase_date
                                                     GROUP BY I.Customer_id,
                                                              I.First_group_purchase_date,
                                                              I.Last_group_purchase_date),
                          Prescription_groups AS (SELECT PH.Customer_id,
                                                         PH.Group_id,
                                                         Round(
                                                                     EXTRACT(
                                                                             EPOCH
                                                                             FROM
                                                                             (
                                                                                 (SELECT Analysis_Formation
                                                                                  FROM date_of_analysis_formation)) -
                                                                             MAX(PH.Transaction_datetime)
                                                                         ) / 60 / 60 / 24,
                                                                     2
                                                             ) AS Prescription_period
                                                  FROM Purchase_history_view PH
                                                           JOIN Periods_view P ON PH.Customer_id = P.Customer_id
                                                      AND PH.Group_id = P.Group_id
                                                  GROUP BY PH.Customer_id,
                                                           PH.Group_id),
                          Stability AS (SELECT Customer_id,
                                               Transaction_id,
                                               Group_ID,
                                               transaction_datetime,
                                               EXTRACT(
                                                       EPOCH
                                                       FROM
                                                       Transaction_datetime - Lag(Transaction_datetime, 1) OVER (
                                                           PARTITION BY Customer_id,
                                                               Group_ID
                                                           ORDER BY
                                                               transaction_datetime
                                                           )
                                                   ) / 60 / 60 / 24 AS Interval_transaction
                                        FROM Purchase_history_view
                                        GROUP BY customer_id,
                                                 transaction_id,
                                                 group_id,
                                                 transaction_datetime
                                        ORDER BY 1,
                                                 3,
                                                 4),
                          Count_stability AS (SELECT P.Customer_id,
                                                     P.Group_id,
                                                     CASE
                                                         WHEN Interval_transaction IS NULL THEN P.Group_frequency
                                                         ELSE Interval_transaction - P.Group_frequency
                                                         END AS Delta
                                              FROM periods_view P
                                                       JOIN Stability S ON P.Customer_id = S.Customer_id
                                                  AND P.Group_id = S.Group_id),
                          Count_delta AS (SELECT P.Customer_id,
                                                 P.Group_id,
                                                 CASE
                                                     WHEN CS.Delta < 0.0 THEN -1.0 * CS.Delta / P.Group_frequency
                                                     ELSE CS.Delta / P.Group_frequency
                                                     END AS Delta_relative
                                          FROM periods_view P
                                                   JOIN Count_stability CS ON P.Customer_id = CS.Customer_id
                                              AND P.Group_id = CS.Group_id),
                          Count_stability_index AS (SELECT Customer_id,
                                                           Group_id,
                                                           AVG(Delta_relative) :: numeric AS Group_stability_index
                                                    FROM Count_delta
                                                    GROUP BY Customer_id,
                                                             Group_id),
                          Real_margin AS (SELECT PH.Customer_id,
                                                 PH.Group_id,
                                                 SUM(
                                                         CASE
                                                             WHEN Calc_method = 1
                                                                 AND
                                                                  PH.Transaction_DateTime BETWEEN date_analysis - Calc_period
                                                                      AND date_analysis
                                                                 THEN PH.Group_summ_paid - PH.group_cost
                                                             WHEN Calc_method = 2
                                                                 AND Number_transactions <= Calc_period
                                                                 THEN PH.Group_summ_paid - PH.Group_cost
                                                             ELSE 0
                                                             END
                                                     ) AS Group_Margin,
                                                 AVG(PH.Group_summ_paid / PH.Group_summ) FILTER (
                                                     WHERE
                                                     PH.Group_summ_paid < PH.Group_summ
                                                     ) AS Group_average_discount
                                          FROM Purchase_history_view PH
                                                   JOIN transactions_date TD ON PH.Customer_id = TD.Customer_id
                                              AND PH.Transaction_id = TD.Transaction_id
                                          GROUP BY PH.Customer_id,
                                                   PH.Group_id),
                          Count_discount AS (SELECT DISTINCT Customer_id,
                                                             Group_id,
                                                             COUNT(*) :: numeric AS Discount_transactions
                                             FROM (SELECT DISTINCT C.customer_id,
                                                                   Group_id,
                                                                   Ch.Transaction_id,
                                                                   SUM(Ch.SKU_Discount) :: numeric AS Summ_discount
                                                   FROM cards C
                                                            JOIN Transactions T ON C.customer_card_id = T.customer_card_id
                                                            JOIN Checks Ch ON T.transaction_id = Ch.transaction_id
                                                            JOIN Product_grid Pg ON Ch.SKU_id = Pg.SKU_id
                                                   GROUP BY C.customer_id,
                                                            Group_id,
                                                            Ch.Transaction_id) AS tmp
                                             WHERE Summ_discount > 0.0
                                             GROUP BY Customer_id,
                                                      Group_id)
                     SELECT P.Customer_id,
                            P.Group_id,
                            P.Group_purchase :: numeric / Ca.Count_transactions :: numeric AS Group_affinity_index,
                            Pg.Prescription_period / P.Group_frequency                     AS Group_churn_rate,
                            Cs.Group_stability_index,
                            R.Group_margin,
                            COALESCE(
                                    (
                                        Cd.Discount_transactions / P.Group_purchase
                                        ),
                                    0
                                )                                                          AS Group_discount_share,
                            P.Group_Min_Discount                                           AS Group_minimum_discount,
                            R.Group_average_discount
                     FROM Periods_view P
                              JOIN Count_all_transactions Ca ON P.Customer_id = Ca.Customer_id
                         AND P.First_group_purchase_date = Ca.First_group_purchase_date
                         AND P.Last_group_purchase_date = Ca.Last_group_purchase_date
                              JOIN Prescription_groups Pg ON P.Customer_id = Pg.Customer_id
                         AND P.Group_id = Pg.Group_id
                              JOIN Count_stability_index Cs ON P.Customer_id = Cs.Customer_id
                         AND P.Group_id = Cs.Group_id
                              JOIN Real_margin R ON P.Customer_id = R.Customer_id
                         AND P.Group_id = R.Group_id
                              JOIN Count_discount Cd ON P.Customer_id = Cd.Customer_id
                         AND P.Group_id = Cd.Group_id;

    END IF;

END;

$$;

CREATE VIEW Groups_view
            (
             Customer_id,
             Group_id,
             Group_affinity_index,
             Group_churn_rate,
             Group_stability_index,
             Group_margin,
             Group_discount_share,
             Group_minimum_discount,
             Group_average_discount
                )
AS
SELECT *
FROM
    fnc_groups_view();

--- TEST CASES
SELECT *
FROM Groups_view
ORDER BY Customer_id,
         Group_id;

SELECT *
FROM Groups_view
WHERE Customer_id = 3;

SELECT *
FROM Groups_view
WHERE group_minimum_discount > 0.2;
