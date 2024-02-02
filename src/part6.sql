--- part 6
--- --- --- доп вьюхи для функции
---
CREATE MATERIALIZED VIEW IF NOT EXISTS sup_customers AS
WITH maininfo AS
         (SELECT PD.customer_id                      AS "CI",
                 TR.transaction_datetime             AS "TD",
                 TR.transaction_store_id             AS "TSI",
                 avg(TR.transaction_summ) OVER w_pci AS "ATS",
                 row_number() OVER w_pci_otd_d       AS rn,
                 count(*) OVER w_pcitsi              AS cnt

          FROM Personal_information AS PD
                   JOIN Cards AS CR ON PD.customer_id = CR.customer_id
                   JOIN Transactions AS TR ON TR.customer_card_id = CR.customer_card_id
              WINDOW w_pci AS (PARTITION BY PD.customer_id),
                  w_pcitsi AS (PARTITION BY PD.customer_id, TR.transaction_store_id),
                  w_pci_otd_d AS (PARTITION BY PD.customer_id ORDER BY TR.transaction_datetime DESC)),
     cte2 AS (SELECT DISTINCT "CI",
                              first_value("TSI") OVER (PARTITION BY "CI" ORDER BY cnt DESC, "TD" DESC) AS preferred_shop,
                              first_value("TSI") OVER (PARTITION BY "CI" ORDER BY rn)                  AS last_shop
              FROM maininfo),
     cte3 AS (SELECT "CI",
                     count(DISTINCT "TSI") last_3_cnt
              FROM maininfo
              WHERE rn <= 3
              GROUP BY "CI")

SELECT "Customer_ID",
       "Customer_Average_Check",
       "Customer_Average_Check_Segment",
       "Customer_Frequency",
       "Customer_Frequency_Segment",
       "Customer_Inactive_Period",
       "Customer_Churn_Rate",
       "Customer_Churn_Segment",
       Segment AS "Segment",

       CASE
           WHEN last_3_cnt = 1 THEN last_shop
           ELSE preferred_shop
           END AS Customer_Primary_Store

FROM (SELECT "Customer_ID",
             "Customer_Average_Check",

             CASE
                 WHEN (percent_rank() OVER w_ocac_d < 0.1) THEN 'High'
                 WHEN (percent_rank() OVER w_ocac_d < 0.35) THEN 'Medium'
                 ELSE 'Low'
                 END                                           AS "Customer_Average_Check_Segment",

             "Customer_Frequency",

             CASE
                 WHEN (percent_rank() OVER w_ocf < 0.1) THEN 'Often'
                 WHEN (percent_rank() OVER w_ocf < 0.35) THEN 'Occasionally'
                 ELSE 'Rarely'
                 END                                           AS "Customer_Frequency_Segment",

             "Customer_Inactive_Period",

             "Customer_Inactive_Period" / "Customer_Frequency" AS "Customer_Churn_Rate",

             CASE
                 WHEN ("Customer_Inactive_Period" / "Customer_Frequency" < 2) THEN 'Low'
                 WHEN ("Customer_Inactive_Period" / "Customer_Frequency" < 5) THEN 'Medium'
                 ELSE 'High'
                 END                                           AS "Customer_Churn_Segment"

      FROM (SELECT "CI"                                    AS "Customer_ID",
                   "ATS"                                   AS "Customer_Average_Check",

                   extract(EPOCH from max("TD") - min("TD"))::float / 86400.0 /
                   count("CI")                             AS "Customer_Frequency",
                   extract(EPOCH from (SELECT analysis_formation FROM date_of_analysis_formation) -
                                      max("TD")) / 86400.0 AS "Customer_Inactive_Period"
            FROM maininfo
            GROUP BY "CI", "ATS"
                WINDOW w_oats_d AS (ORDER BY sum("ATS") DESC)) AS avmain
      GROUP BY "Customer_ID",
               "Customer_Average_Check",
               "Customer_Frequency",
               "Customer_Inactive_Period"
          WINDOW w_ocac_d AS (ORDER BY sum("Customer_Average_Check") DESC),
              w_ocf AS (ORDER BY "Customer_Frequency")) AS biginfo
         JOIN Segments AS S ON (S.Average_Check = "Customer_Average_Check_Segment" AND
                                S.frequency_of_purchases = "Customer_Frequency_Segment" AND
                                S.Churn_Probability = "Customer_Churn_Segment")
         JOIN cte2 ON cte2."CI" = biginfo."Customer_ID"
         JOIN cte3 ON cte3."CI" = biginfo."Customer_ID";

CREATE MATERIALIZED VIEW IF NOT EXISTS sup_data AS
SELECT CR.customer_id,
       TR.transaction_id,
       TR.transaction_datetime,
       TR.transaction_store_id,
       SKU.group_id,
       CK.sku_amount,
       SR.sku_id,
       SR.sku_retail_price,
       SR.sku_purchase_price,
       CK.sku_summ_paid,
       CK.sku_summ,
       CK.sku_discount
FROM transactions AS TR
         JOIN cards AS CR ON CR.customer_card_id = TR.customer_card_id
         JOIN personal_information AS PD ON PD.customer_id = CR.customer_id
         JOIN checks AS CK ON TR.transaction_id = CK.transaction_id
         JOIN sku_group AS SKU ON SKU.group_id = CK.sku_id
         JOIN stores AS SR ON SKU.group_id = SR.sku_id AND
                              TR.transaction_store_id = SR.transaction_store_id;

---
--- Функция для формирования персональных предложений, ориентированных на кросс-продажи
---
CREATE
    OR REPLACE FUNCTION cross_selling(
    IN cnt_group int,
    IN max_churn_rate numeric,
    IN max_stability_index numeric,
    IN max_sku_share numeric,
    IN max_margin_share numeric
)
    RETURNS TABLE
            (
                Customer_ID          INT,
                SKU_Name             TEXT,
                Offer_Discount_Depth int
            )
AS
$fnc_formation_personal_offers_cross_selling$
BEGIN
    RETURN QUERY SELECT DISTINCT MD."Customer_ID",
                                 MD.SN,
                                 CASE
                                     WHEN (MD.group_minimum_discount * 1.05 * 100)::int = 0 THEN 5
                                     ELSE (MD.group_minimum_discount * 1.05 * 100)::int
                                     END
                 FROM (SELECT dense_rank() OVER (PARTITION BY VG.customer_id ORDER BY VG.group_id) AS DR,
                              first_value(sku_group.group_name) OVER (
                                  PARTITION BY VG.customer_id, VG.group_id
                                  ORDER BY (VB.sku_retail_price - VB.sku_purchase_price) DESC)     AS SN,
                              VG.group_id                                                          AS GI,
                              *
                       FROM groups_view AS VG
                                JOIN sup_data AS VB ON VB.customer_id = VG.Customer_ID AND VB.group_id = VG.group_id
                                JOIN sup_customers AS VC ON VC."Customer_ID" = VG.customer_id
                                JOIN sku_group ON sku_group.group_id = VG.group_id AND sku_group.group_id = VB.sku_id
                       WHERE VC.customer_primary_store = VB.transaction_store_id
                         AND VG.group_churn_rate <= max_churn_rate
                         AND VG.group_stability_index < max_stability_index) AS MD
                 WHERE DR <= cnt_group
                   AND (SELECT count(*) FILTER ( WHERE sku_group.group_name = MD.SN)::numeric / count(*)
                        FROM sup_data AS VB
                                 JOIN sku_group ON sku_group.group_id = VB.sku_id
                        WHERE VB.customer_id = MD."Customer_ID"
                          AND VB.group_id = MD.GI) < max_sku_share
                   AND (MD.sku_retail_price - MD.sku_purchase_price) * max_margin_share / 100.0 / MD.sku_retail_price >=
                       MD.group_minimum_discount * 1.05;
END ;
$fnc_formation_personal_offers_cross_selling$
    LANGUAGE plpgsql;

SELECT *
FROM cross_selling(100, 100, 100, 2, 100);
