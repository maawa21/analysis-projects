SELECT*
FROM dairy_dataset;

CREATE TABLE dairy_dataset_staging
LIKE dairy_dataset;

SELECT*
FROM dairy_dataset_staging;

INSERT dairy_dataset_staging
SELECT*
FROM dairy_dataset;

SELECT*
FROM dairy_dataset_staging;

-- REMOVING DUPLICATES --
ALTER TABLE dairy_dataset_staging
RENAME COLUMN `Quantity_liters\kg` TO `Quantity_liters/kg`;
           
SELECT*
FROM dairy_dataset_staging;

SELECT*,
ROW_NUMBER() OVER( PARTITION BY Location, `Total_Land_Area_acres` ,Number_of_Cows,Farm_Size,`Date`,
Product_ID,Product_Name,Brand,Price_per_Unit,Total_Value,Sales_Channel,Customer_Location,Production_Date,Storage_Condition) AS Row_Num
FROM dairy_dataset_staging;

WITH duplicate_CTE AS 
(
SELECT*,
ROW_NUMBER() OVER( PARTITION BY Location, `Total_Land_Area_acres` ,Number_of_Cows,Farm_Size,`Date`,
Product_ID,Product_Name,Brand,Price_per_Unit,Total_Value,Sales_Channel,Customer_Location,Production_Date,Storage_Condition) AS Row_Num
FROM dairy_dataset_staging
)
SELECT*
FROM duplicate_CTE
WHERE Row_Num > 1;

SELECT*
FROM dairy_dataset_staging;

SELECT distinct(Brand) 
FROM dairy_dataset_staging;

SELECT `date`
FROM dairy_dataset_staging;

ALTER TABLE dairy_dataset_staging
MODIFY COLUMN `Date` DATE;

SELECT*
FROM dairy_dataset_staging
WHERE Date IS NULL;

-- EXPLORATION DATA --

SELECT*
FROM dairy_dataset_staging;

SELECT MAX(Total_Land_Area_acres),MAX(Price_per_Unit)
FROM dairy_dataset_staging;

SELECT brand, Total_Land_Area_acres,Number_of_Cows, Price_per_Unit
FROM dairy_dataset_staging
ORDER BY Price_per_Unit desc;


SELECT brand, SUM(Price_per_Unit)
FROM dairy_dataset_staging
GROUP BY brand 
ORDER BY 2 desc;

SELECT brand, SUM(Price_per_Unit),SUM(`Quantity_Sold_liters/kg`)
FROM dairy_dataset_staging
GROUP BY brand 
;

SELECT MIN(`Date`), max(`date`)
from dairy_dataset_staging;

SELECT YEAR(`date`), brand, sum(`Quantity_Sold_liters/kg`)
from dairy_dataset_staging
GROUP BY year(`date`), brand
ORDER BY 1 desc;

ALTER TABLE dairy_dataset_staging
RENAME COLUMN `Quantity_Sold_liters/kg` TO `Quantity_Sold_liters_kg`;
           

SELECT*
FROM dairy_dataset_staging;

WITH brand_year ( brand , years , Total_Quantity_Sold ) AS
(
SELECT brand, YEAR(`date`)AS YEARS, sum(Quantity_Sold_liters_kg) AS Total_Quantity_Sold
from dairy_dataset_staging
GROUP BY year(`date`), brand
ORDER BY 1 desc
),
brand_year_rank AS
(
select*,
dense_rank() OVER( partition by YEARS ORDER BY Total_Quantity_Sold desc) AS RANKING
FROM brand_year
)
select *
FROM brand_year_rank
WHERE ranking <=5 ;

CREATE PROCEDURE land_area_large()
SELECT Total_Land_Area_acres
FROM dairy_dataset_staging
WHERE Total_Land_Area_acres >= 700 ;

CALL land_area_large();

SELECT Total_Land_Area_acres,
CASE
	WHEN Total_Land_Area_acres <= 299.99 THEN 'Small'
    WHEN Total_Land_Area_acres <700 THEN 'Medium'
    WHEN Total_Land_Area_acres >= 700 THEN 'Large'
    END AS Land_Category
FROM dairy_dataset_staging;


ALTER TABLE dairy_dataset_staging ADD COLUMN Land_category varchar(20);

UPDATE dairy_dataset_staging
SET Land_category = CASE
	WHEN Total_Land_Area_acres <= 299.99 THEN 'Small'
    WHEN Total_Land_Area_acres <700 THEN 'Medium'
    WHEN Total_Land_Area_acres >= 700 THEN 'Large'
    END ;

SELECT SUM(Quantity_Sold_liters_kg)
FROM dairy_dataset_staging
where brand = 'mother dairy' 
and date <= '2020-01-01' ;

SELECT*
FROM dairy_dataset_staging;


SELECT  year(`date`),brand,SUM(Quantity_Sold_liters_kg), AVG(Price_per_Unit),MAX(Price_per_Unit), MIN(Price_per_Unit) 
FROM dairy_dataset_staging 
GROUP BY year(`date`),brand;

SELECT  year(`date`),brand,Product_Name,SUM(Quantity_Sold_liters_kg), AVG(Price_per_Unit),MAX(Price_per_Unit), MIN(Price_per_Unit) 
FROM dairy_dataset_staging
where brand= 'amul' and year(`date`)<= '2022-12-31'
GROUP BY year(`date`),brand, Product_Name
;


WITH Total_sold_year ( years , brand, product_name, sum_sold, avg_price, max_price, min_price ) AS
(
SELECT  year(`date`) AS years ,brand,Product_Name,
SUM(Quantity_Sold_liters_kg) AS sum_sold, 
AVG(Price_per_Unit) AS avg_price,
MAX(Price_per_Unit) AS max_price, 
MIN(Price_per_Unit) AS min_price
FROM dairy_dataset_staging
where brand= 'mother dairy' 
GROUP BY years,brand, Product_Name
),
total_sold_year_rank AS
(
select*,
dense_rank() OVER( partition by years ORDER BY sum_sold desc) AS RANKING
FROM total_sold_year
)
select *
FROM total_sold_year_rank
WHERE ranking <=5 ;



WITH Total_sold_year ( years , brand, product_name, sum_sold, avg_price, max_price, min_price ) AS
(
SELECT  year(`date`) AS years ,brand,Product_Name,
SUM(Quantity_Sold_liters_kg) AS sum_sold, 
AVG(Price_per_Unit) AS avg_price,
MAX(Price_per_Unit) AS max_price, 
MIN(Price_per_Unit) AS min_price
FROM dairy_dataset_staging
GROUP BY years,brand, Product_Name

),
total_sold_year_rank AS
(
select*,
dense_rank() OVER( partition by years ORDER BY sum_sold desc) AS RANKING
FROM total_sold_year
)
select *
FROM total_sold_year_rank
WHERE ranking <=5 ;

SELECT brand, SUM(Number_of_Cows), YEAR(`date`)
FROM dairy_dataset_staging
GROUP BY YEAR(`date`),brand ;


SELECT brand, sum(Approx_Total_RevenueINR), AVG(Approx_Total_RevenueINR),SUM(Quantity_Sold_liters_kg),
sum(Price_per_Unit_sold) , SUM(Quantity_Sold_liters_kg)*sum(Price_per_Unit_sold) AS revenue
FROM dairy_dataset_staging
GROUP BY brand ;

SELECT brand,sum(Price_per_Unit_sold)
FROM dairy_dataset_staging
group by brand;