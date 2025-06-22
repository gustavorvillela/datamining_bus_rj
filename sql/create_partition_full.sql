-- Renomear a tabela antiga
--ALTER TABLE onibus_rj RENAME TO onibus_rj_old;

-- Criar a tabela mãe particionada
CREATE TABLE onibus_rj (
    id SERIAL,
    ordem TEXT,
    linha TEXT,
    velocidade DOUBLE PRECISION,
    datahora TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    geom geometry(Point, 4326)
) PARTITION BY RANGE (datahora);
