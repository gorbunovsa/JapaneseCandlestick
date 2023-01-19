# JapaneseCandlestick

Converting data to Japanese candlestick format using Hadoop MapReduce Java API

Столбцы файла с финансовыми данными имеют следующие названия:
#SYMBOL,SYSTEM,MOMENT,ID_DEAL,PRICE_DEAL,VOLUME,OPEN_POS,DIRECTION
где #SYMBOL — название финансового инструмента;
MOMENT — время (дата);
PRICE_DEAL — цена.

Пример строки в файле:
SVH1,F,20110111100000080,255223067,30.46000,1,8714,S

Пример финансовых данных: /src/main/resources/fin_sample.csv

Параметры конфигурации имеют следующие значения по умолчанию:
- candle.width = 300000 — ширина свечи в числе миллисекунд;
- candle.securities = .* — шаблон инструментов — задается в виде регулярного выражения;
- candle.date.from = 19000101 — первый день периода времени (ГГГГММДД);
- candle.date.to = 2001 — первый день после последнего дня периода (ГГГГММДД);
- candle.time.from = 1000 — время (ЧЧММ) начала первой свечи; 
- candle.time.to = 1800 — время (ЧЧММ) после окончания последней свечи;
- candle.num.reducers = 1 — число редьюсеров.

