-- ===============================
-- 01 - Verificar quantas linhas únicas de ônibus existem
-- ===============================
SELECT DISTINCT linha FROM onibus_rj;

-- ===============================
-- 02 - Contagem de registros por linha
-- ===============================
SELECT linha, COUNT(*) AS total_registros
FROM onibus_rj
GROUP BY linha
ORDER BY total_registros DESC;

-- ===============================
-- 03 - Verificar intervalo de datas na base
-- ===============================
SELECT MIN(datahora) AS primeira_data, MAX(datahora) AS ultima_data
FROM onibus_rj;

-- ===============================
-- 04 - Verificar intervalo de velocidade
-- ===============================
SELECT MIN(velocidade) AS velocidade_minima, MAX(velocidade) AS velocidade_maxima, AVG(velocidade) AS velocidade_media
FROM onibus_rj;

-- ===============================
-- 05 - Amostra de dados (10 primeiros registros aleatórios)
-- ===============================
SELECT *
FROM onibus_rj
ORDER BY RANDOM()
LIMIT 10;

-- ===============================
-- 06 - Quantidade de registros por ônibus (ordem)
-- ===============================
SELECT ordem, COUNT(*) AS total_por_onibus
FROM onibus_rj
GROUP BY ordem
ORDER BY total_por_onibus DESC;

-- ===============================
-- 07 - Detectar velocidades fora do esperado (possíveis outliers de velocidade)
-- ===============================
SELECT *
FROM onibus_rj
WHERE velocidade < 0 OR velocidade > 120;

-- ===============================
-- 08 - Contar registros com velocidade = 0 (ônibus parado)
-- ===============================
SELECT COUNT(*) AS total_parados
FROM onibus_rj
WHERE velocidade = 0;

-- ===============================
-- 09 - Extrair amostra de uma linha específica (exemplo: linha '232')
-- ===============================
SELECT *
FROM onibus_rj
WHERE linha = '232'
LIMIT 1000;

-- ===============================
-- 10 - Extraindo Latitude e Longitude (para uso posterior no Python com Geopandas)
-- ===============================
SELECT id, ordem, linha, velocidade, datahora,
       ST_X(geom) AS longitude,
       ST_Y(geom) AS latitude
FROM onibus_rj
LIMIT 1000;

-- ===============================
-- 11 - Contagem de registros por dia (para entender distribuição temporal)
-- ===============================
SELECT DATE(datahora) AS dia, COUNT(*) AS total
FROM onibus_rj
GROUP BY dia
ORDER BY dia;
