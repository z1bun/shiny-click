docker run -d -p 8080:80 spoonest/clickhouse-tabix-web-client

Создаем таблицу с посещениями пользователей

CREATE TABLE IF NOT EXISTS `visits`  (
  `VisitDate` Date,
  `Year` UInt16,
  `Month` UInt8,
  `DayofMonth` UInt8,
  `DayOfWeek` UInt8,
  `UserId` UInt64,
  `CountryId` UInt32,
  -- some other data
  `VisitedUrl` String
) ENGINE = MergeTree()
PARTITION BY Year
ORDER BY tuple() 
SETTINGS index_granularity = 8192

Записывать данные о каждом посещении в лог будем по следующей схеме: 

INSERT INTO visits VALUES(VisitDate, ...)

Задача: написать запрос на получение статистики по посещаемости сайта по дням за последний месяц в разрезе стран, из которых пользователи заходили на сайт.

a) Запрос за предыдущий месяц

SELECT DayofMonth, CountryId, count(*) AS c 
FROM visits 
WHERE Month = ((toMonth(toStartOfMonth(now( ))) - 1 == 0) ? 12 : (toMonth(toStartOfMonth(now( ))) - 1)) AND Year = toYear(now()) 
GROUP BY DayofMonth, CountryId 
ORDER BY DayofMonth, CountryId ASC


b) Запрос за текущий месяц

SELECT DayofMonth, CountryId, count(*) AS c 
FROM visits 
WHERE Month = toMonth(now()) AND Year = toYear(now()) 
GROUP BY DayofMonth, CountryId 
ORDER BY DayofMonth, CountryId ASC

Так как партицирование у нас происходит по годам, то ежегодно удаляем старые данные так:
ALTER TABLE visits DROP PARTITION toYear(now()) - 1;

