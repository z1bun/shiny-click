# Создаем таблицу с посещениями пользователей  
```
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
```
## Записывать данные о каждом посещении в лог будем по следующей схеме: 
```
INSERT INTO visits VALUES(VisitDate, ...)
```
Для аналитики будем записывать данные большими объемами средствами клиента. Данные будут уже поделены (то есть VisitDate - общее поле, Year, Month и другие поля с датами - служебные поля). Для стран используем ещё одну таблицу - country, которая будет выглядеть, например, так:

```
CREATE TABLE IF NOT EXISTS `country`  (  
  `Id` Uint16,
  `Name` String
) ENGINE = Log
```
# Задача   
Написать запрос на получение статистики по посещаемости сайта по дням за последний месяц в разрезе стран, из которых пользователи заходили на сайт.

## a) Запрос за предыдущий месяц
```
SELECT DayofMonth, CountryId, count(*) AS c 
FROM visits 
WHERE Month = ((toMonth(toStartOfMonth(now( ))) - 1 == 0) ? 12 : (toMonth(toStartOfMonth(now( ))) - 1)) AND Year = toYear(now()) 
GROUP BY DayofMonth, CountryId 
ORDER BY DayofMonth, CountryId ASC
```

## b) Запрос за текущий месяц
```
SELECT DayofMonth, CountryId, count(*) AS c 
FROM visits 
WHERE Month = toMonth(now()) AND Year = toYear(now()) 
GROUP BY DayofMonth, CountryId 
ORDER BY DayofMonth, CountryId ASC
```
## Удаление устаревших данных
Так как партицирование у нас происходит по годам, то ежегодно удаляем старые данные так:
```ALTER TABLE visits DROP PARTITION toYear(now()) - 1;```

