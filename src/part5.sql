--- part 5
--- --- --- Формирование персональных предложений, ориентированных на рост частоты визитов
---
CREATE OR REPLACE FUNCTION personal_offers_aimed_increasing_frequency(date_begin TIMESTAMP,
                                                                      date_end TIMESTAMP,
                                                                      adding_transactions_count INT,
                                                                      max_churn NUMERIC,
                                                                      max_discount_share NUMERIC,
                                                                      max_marge_share NUMERIC)
    RETURNS TABLE
            (
                customer_id                 INT,
                start_date                  TIMESTAMP,
                end_date                    TIMESTAMP,
                required_transactions_count INT,
                group_name                  TEXT,
                offer_discount_depth        INT
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    IF date_begin > end_date THEN
        RAISE EXCEPTION 'ERROR: The start date must be earlier than the end date';
    END IF;
    RETURN QUERY
        SELECT customers_view.customer_id AS customer_id,
               date_begin                 AS Start_Date,
               date_end                   AS End_Date,
               -- преобразовать каждое значение в количество секунд, найти разницу результатов
               ROUND(EXTRACT(EPOCH FROM (date_end - date_begin) / 86400::NUMERIC) /
                     customers_view.customer_frequency)::INT +
               adding_transactions_count  AS Required_Transactions_Count,
               sku_group.group_name       AS group_name,
               discount.discount          AS Offer_Discount_Depth
        FROM customers_view
                 JOIN (SELECT *
                       FROM get_discount(max_churn, max_discount_share,
                                         max_marge_share)) discount
                      ON customers_view.customer_id = discount.customer_id
                 JOIN sku_group
                      ON sku_group.group_id = discount.group_id;
END;
$$;


-- TEST CASES
SELECT *
FROM personal_offers_aimed_increasing_frequency('2022-08-19 00:00:00.0000000',
                                                '2022-08-18 00:00:00.0000000',
                                                4, 3, 70, 30);
SELECT *
FROM personal_offers_aimed_increasing_frequency('2021-02-19 00:00:00.0000000',
                                                '2024-10-18 00:00:00.0000000',
                                                1, 8, 60, 1000);
