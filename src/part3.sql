--- part 3
--- --- --- Добавление ролей администратора и посетителя
---
CREATE ROLE administrator SUPERUSER CREATEDB CREATEROLE LOGIN PASSWORD 'qwerty';

GRANT ALL PRIVILEGES ON DATABASE "postgres" TO administrator;
GRANT pg_signal_backend,
    pg_execute_server_program TO administrator;

CREATE ROLE visitor LOGIN;
GRANT CONNECT ON DATABASE "postgres" TO visitor;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO visitor;

-- TEST CASE
SELECT *
FROM pg_roles
WHERE LEFT(rolname, 2) IN ('ad', 'vi');

--DROP OWNED By administrator;
--DROP ROLE administrator;
--DROP OWNED By visitor;
--DROP ROLE visitor;