---  part 4
---  Функция для расчета среднего чека по периоду
CREATE OR REPLACE FUNCTION get_period_table(date_begin DATE, date_end DATE,
                                            cef_increase_check NUMERIC)
    RETURNS TABLE
            (
                customer_id            INT,
                required_check_measure NUMERIC
            )
    LANGUAGE plpgsql
AS
$$
BEGIN

    RETURN QUERY SELECT personal_information.customer_id AS customer_id,
                        SUM(t.transaction_summ) / COUNT(*) *
                        cef_increase_check               AS required_check_measure
                 FROM personal_information
                          JOIN cards ON personal_information.customer_id = cards.customer_id
                          JOIN transactions t ON t.customer_card_id = cards.customer_card_id
                 WHERE transaction_datetime::date >= date_begin
                   AND transaction_datetime::date <= date_end
                 GROUP BY personal_information.customer_id;
END;
$$;


--- TEST CASES
SELECT *
FROM get_period_table('2018-06-11', '2018-11-13', 15);
SELECT *
FROM get_period_table('2017-11-11', '2019-11-13', 65);


---  Функция для расчета среднего чека по количеству
CREATE OR REPLACE FUNCTION get_transaction_table(transactions_count INT,
                                                 cef_increase_check NUMERIC)
    RETURNS TABLE
            (
                customer_id            INT,
                required_check_measure NUMERIC
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        SELECT query.customer_id                                                  AS customer_id,
               SUM(transaction_summ) / COUNT(transaction_id) * cef_increase_check AS Required_Check_Measure
        FROM (SELECT p.customer_id,
                     transaction_id,
                     transaction_summ,
                     ROW_NUMBER()
                     OVER (PARTITION BY p.customer_id ORDER BY transaction_datetime DESC) AS count
              FROM personal_information p
                       JOIN cards ON p.customer_id = cards.customer_id
                       JOIN transactions ON transactions.customer_card_id = cards.customer_card_id) query
        WHERE count <= transactions_count
        GROUP BY query.customer_id;
END;
$$;

--- TEST CASE
SELECT *
FROM get_transaction_table(21, 2.7);


CREATE OR REPLACE FUNCTION get_discount(max_churn NUMERIC,
                                        max_discount_share NUMERIC,
                                        max_marge_share NUMERIC)
    RETURNS TABLE
            (
                customer_id INT,
                group_id    INT,
                discount    INT
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        WITH t1 AS (SELECT purchase_history_view.customer_id,
                           purchase_history_view.group_id,
                           CASE
                               WHEN ((SUM(purchase_history_view.group_summ_paid - purchase_history_view.group_cost) /
                                      COUNT(*)) < 0) THEN 0
                               ELSE (SUM(purchase_history_view.group_summ_paid - purchase_history_view.group_cost) /
                                     SUM(purchase_history_view.group_cost) * max_marge_share
                                   ) END AS marge
                    FROM purchase_history_view
                    GROUP BY purchase_history_view.customer_id, purchase_history_view.group_id),

             t2 AS (SELECT groups_view.customer_id,
                           groups_view.group_id,
                           groups_view.group_affinity_index,
                           (groups_view.group_minimum_discount * 100)::int / 5 * 5 + 5 AS                             Discount,
                           ROW_NUMBER()
                           OVER (PARTITION BY groups_view.customer_id ORDER BY groups_view.group_affinity_index DESC) raiting
                    FROM groups_view
                    WHERE groups_view.group_churn_rate <= max_churn
                      AND groups_view.group_discount_share < (max_discount_share / 100::NUMERIC)),
             t3 AS (SELECT t1.customer_id,
                           t1.group_id,
                           CASE
                               -- PARTITION BY позволяет сгруппировать строки по значению определённого столбца
                               WHEN (t2.raiting = MIN(t2.raiting) OVER (PARTITION BY t1.customer_id )) THEN t2.Discount
                               END AS Discount
                    FROM t1
                             JOIN t2
                                  ON (t1.customer_id, t1.group_id) = (t2.customer_id, t2.group_id)
                    WHERE t2.Discount < marge)
        SELECT *
        FROM t3
        WHERE t3.discount IS NOT NULL;
END;
$$;

--- TEST CASES
SELECT *
FROM get_discount(3, 70, 30);

--- Формирование персональных предложений, ориентированных на рост среднего чека
CREATE
    OR REPLACE FUNCTION offers_of_the_average_check(calc_method INT, first_date DATE, end_date DATE,
                                                    trans_number INT,
                                                    increase_coefficient NUMERIC, max_index NUMERIC, max_share NUMERIC,
                                                    allow_share NUMERIC)
    RETURNS TABLE
            (
                customer_id            INT,
                required_check_measure NUMERIC,
                group_name             TEXT,
                offer_discount_depth   INT
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    IF first_date > end_date THEN
        RAISE EXCEPTION 'ERROR: The start date must be earlier than the end date';
    END IF;
    DROP TABLE IF EXISTS tmp;
    CREATE TEMP TABLE tmp
    (
        customer               INT,
        required_check_measure NUMERIC
    );
    IF calc_method = 1 THEN
        INSERT INTO tmp SELECT * FROM get_period_table(first_date, end_date, increase_coefficient);
    ELSIF calc_method = 2 THEN
        INSERT INTO tmp SELECT * FROM get_transaction_table(trans_number, increase_coefficient);
    END IF;

    RETURN QUERY
        SELECT discount.customer_id       AS customer_id,
               tmp.Required_Check_Measure AS Required_Check_Measure,
               sku_group.group_name       AS group_name,
               discount.Discount          AS Offer_Discount_Depth

        FROM (SELECT *
              FROM get_discount(max_index, max_share,
                                allow_share)) discount
                 JOIN sku_group
                      ON sku_group.group_id = discount.group_id
                 JOIN tmp ON tmp.customer = discount.customer_id;

END;
$$;

--- TEST CASES
SELECT *
FROM offers_of_the_average_check(2, '2012-01-01', '2023-01-01', 10, 12, 1, 70, 30);
SELECT *
FROM offers_of_the_average_check(2, '2012-01-01', '2023-01-01', 100, 1.15, 3, 70, 30);
