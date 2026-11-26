-- Generic row shape used by the relational task examples.
-- Select the correct existing database/schema and review endpoint requirements.
-- MySQL/MariaDB require InnoDB; PostgreSQL requires an ordinary permanent table.
-- CDC EX does not execute this template.
CREATE TABLE orders
(
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR(120) NOT NULL,
    amount DECIMAL(18,2) NOT NULL
);
