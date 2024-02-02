--- part 1
--- --- --- Создание и заполнение всех таблиц
---
DROP SCHEMA IF EXISTS PUBLIC CASCADE;

CREATE SCHEMA PUBLIC;

SET DATESTYLE TO 'DMY';

---
--- --- --- Создание таблиц
---

--- Personal_information
CREATE TABLE IF NOT EXISTS Personal_information
(
    Customer_ID            INT NOT NULL PRIMARY KEY,
    Customer_Name          VARCHAR CHECK (Customer_Name ~* '^([A-Z])[a-z\-\ ]+$|([А-Я])[а-я\-\ ]+$'),
    Customer_Surname       VARCHAR CHECK (Customer_Surname ~* '^([A-Z])[a-z\-\ ]+$|([А-Я])[а-я\-\ ]+$'),
    Customer_Primary_Email VARCHAR CHECK (Customer_Primary_Email ~*
                                          '^([a-zA-Z0-9_\-\.]+)@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.)|(([a-zA-Z0-9\-]+\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\]?)$'),
    Customer_Primary_Phone TEXT CHECK (Customer_Primary_Phone ~* '^((\+7)+([0-9]){10})$')
);

--- CARDS
CREATE TABLE IF NOT EXISTS Cards
(
    Customer_Card_ID INT NOT NULL PRIMARY KEY,
    Customer_ID      INT NOT NULL,
    CONSTRAINT fk_customer FOREIGN KEY (Customer_ID) REFERENCES Personal_information (Customer_ID)
);

--- SKU_group
CREATE TABLE IF NOT EXISTS SKU_group
(
    Group_ID   INT NOT NULL PRIMARY KEY,
    Group_Name TEXT CHECK (Group_Name ~* '([A-Za-z0-9А-Яа-я?!\d+_@.-])') -- works with ñ
);

--- Product_grid
CREATE TABLE IF NOT EXISTS Product_grid
(
    SKU_ID   INT,
    CONSTRAINT fk_sku UNIQUE (SKU_ID),
    SKU_Name TEXT CHECK (SKU_Name ~* '([A-Za-z0-9А-Яа-я?!\d+_@.-])'), -- works with ñ
    Group_ID INT,
    CONSTRAINT fk_group FOREIGN KEY (Group_ID) REFERENCES SKU_group (Group_ID)
);

--- Transactions
CREATE TABLE IF NOT EXISTS Transactions
(
    Transaction_ID       INT NOT NULL PRIMARY KEY,
    Customer_Card_ID     INT,
    CONSTRAINT fk_customercard FOREIGN KEY (Customer_Card_ID) REFERENCES Cards (Customer_Card_ID),
    Transaction_Summ     NUMERIC,
    Transaction_DateTime TIMESTAMP WITHOUT TIME ZONE,
    Transaction_Store_ID INT
);

--- Stores
CREATE TABLE IF NOT EXISTS Stores
(
    Transaction_Store_ID INT NOT NULL,
    SKU_ID               INT,
    CONSTRAINT fk_sku_st FOREIGN KEY (SKU_ID) REFERENCES Product_grid (SKU_ID),
    SKU_Purchase_Price   NUMERIC,
    SKU_Retail_Price     NUMERIC
);

--- Checks
CREATE TABLE IF NOT EXISTS Checks
(
    Transaction_ID INT,
    CONSTRAINT fk_transaction FOREIGN KEY (Transaction_ID) REFERENCES Transactions (Transaction_ID),
    SKU_ID         INT,
    CONSTRAINT fk_sku_ch FOREIGN KEY (SKU_ID) REFERENCES Product_grid (SKU_ID),
    SKU_Amount     NUMERIC,
    SKU_Summ       NUMERIC,
    SKU_Summ_Paid  NUMERIC,
    SKU_Discount   NUMERIC
);

--- date_of_analysis_formation
CREATE TABLE IF NOT EXISTS date_of_analysis_formation
(
    Analysis_Formation TIMESTAMP WITHOUT TIME ZONE
);

--- segments
CREATE TABLE IF NOT EXISTS segments
(
    SEGMENT                INTEGER,
    Average_check          VARCHAR(25) NOT NULL CHECK (Average_check IN ('Low', 'Medium', 'High')),
    Frequency_of_purchases VARCHAR(25) NOT NULL CHECK (
        Frequency_of_purchases IN ('Often', 'Occasionally', 'Rarely')
        ),
    Churn_probability      VARCHAR(25) NOT NULL CHECK (Churn_probability IN ('Low', 'Medium', 'High')),
    CONSTRAINT unique_Average_check_Frequency_of_purchases_Churn_probability UNIQUE (
                                                                                     Average_check,
                                                                                     Frequency_of_purchases,
                                                                                     Churn_probability
        )
);

---
--- --- --- Импорт данных из файлов в таблицы
---
COPY Personal_Information
    FROM '/Users/dionecar/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Personal_Data_Mini.tsv'
    DELIMITER E'\t'
    CSV;

COPY Cards
    FROM '/Users/dionecar/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Cards_Mini.tsv'
    DELIMITER E'\t'
    CSV;

COPY SKU_group
    FROM '/Users/dionecar/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Groups_SKU_Mini.tsv'
    DELIMITER E'\t'
    CSV;

COPY Product_grid
    FROM '/Users/dionecar/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/SKU_Mini.tsv'
    DELIMITER E'\t'
    CSV;

COPY Stores
    FROM '/Users/dionecar/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Stores_Mini.tsv'
    DELIMITER E'\t'
    CSV;

COPY Transactions
    FROM '/Users/dionecar/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Transactions_Mini.tsv'
    DELIMITER E'\t'
    CSV;

COPY Checks
    FROM '/Users/dionecar/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Checks_Mini.tsv'
    DELIMITER E'\t'
    CSV;

COPY date_of_analysis_formation
    FROM '/Users/dionecar/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Date_Of_Analysis_Formation.tsv'
    DELIMITER E'\t'
    CSV;

COPY Segments
    FROM '/Users/dionecar/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Segments.tsv'
    DELIMITER E'\t'
    CSV;

---
--- --- --- Экспорт данных из таблиц в файлы
---
CREATE
    OR REPLACE PROCEDURE write_to(table_name VARCHAR) AS
$$
DECLARE
    -- Прописать свой каталог (pwd в терминале)
    -- Дать права в терминале на чтение/запись в этот каталог:
    -- chmod -R 777 /Users/dionecar/Desktop/SQL3_RetailAnalitycs_v1.0-1/misc/
    my_dir    VARCHAR := '/Users/dionecar/Desktop/SQL3_RetailAnalitycs_v1.0-1/misc/';
    separator CHAR    := E'\t';

BEGIN
    EXECUTE FORMAT(
            'copy %s to ''%s'' delimiter ''%s'' csv',
            table_name,
            CONCAT(
                    my_dir,
                    CONCAT(LOWER(table_name), '.csv')
                ),
            separator
        );

END
$$ LANGUAGE plpgsql;

--- Сохраняем в файлы
DO
$save_tables$
    BEGIN
        CALL write_to('Cards');
        CALL write_to('Checks');
        CALL write_to('date_of_analysis_formation');
        CALL write_to('Personal_Information');
        CALL write_to('Product_grid');
        CALL write_to('SKU_group');
        CALL write_to('Stores');
        CALL write_to('Transactions');
    END;
$save_tables$;
