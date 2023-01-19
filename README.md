# JapaneseCandlestick

Converting data to Japanese candlestick format using Hadoop MapReduce Java API

Пример финансовых данных: /src/main/resources/fin_sample.csv

Параметры конфигурации имеют следующие значения по умолчанию:
- candle.width = 300000 — ширина свечи в числе миллисекунд;
- candle.securities = .* — шаблон инструментов — задается в виде регулярного выражения;
- candle.date.from = 19000101 — первый день периода времени (ГГГГММДД);
- candle.date.to = 2001 — первый день после последнего дня периода (ГГГГММДД);
- candle.time.from = 1000 — время (ЧЧММ) начала первой свечи; 
- candle.time.to = 1800 — время (ЧЧММ) после окончания последней свечи;
- candle.num.reducers = 1 — число редьюсеров.

