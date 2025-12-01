SELECT*
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT*
FROM layoffs_staging;

INSERT layoffs_staging
SELECT*
FROM layoffs;

SELECT*
FROM layoffs;

SELECT*
FROM layoffs_staging;


-- removing dublicates-- 

SELECT*,
ROW_NUMBER() OVER()
FROM layoffs_staging;

SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 'date', total_laid_off,percentage_laid_off, country, funds_raised_millions ) AS row_num
FROM layoffs_staging;



WITH duplicate_cte AS
( 
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging
)
SELECT*
FROM duplicate_CTE
WHERE row_num >1 ;

SELECT*
FROM layoffs_staging
WHERE company = 'tiktok' ; 

SELECT*
FROM layoffs_staging
WHERE company = 'casper' ; 

WITH duplicate_cte AS
( 
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, `date`,
 total_laid_off,percentage_laid_off, country, funds_raised_millions ) AS row_num
FROM layoffs_staging
)
SELECT*
FROM duplicate_CTE
WHERE row_num >1 ;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT*
FROM layoffs_staging2;

INSERT layoffs_staging2
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, `date`,
 total_laid_off,percentage_laid_off, country, funds_raised_millions ) AS row_num
FROM layoffs_staging;

SELECT*
FROM layoffs_staging2
WHERE row_num > 1;


DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT*
FROM layoffs_staging
WHERE company = 'tiktok' ; 

SELECT*
FROM layoffs_staging2
WHERE company = 'casper' ;

-- standardizing data --

-- finding issues in data and fixing it -- 

select*
from layoffs_staging2;

SELECT company
FROM layoffs_staging2; 

SELECT DISTINCT (company)
FROM layoffs_staging2; 

select company, TRIM(company)
from layoffs_staging2;


UPDATE layoffs_staging2
SET company = TRIM(company);


select*
from layoffs_staging2;

select industry
from layoffs_staging2;


select DISTINCT(industry)
from layoffs_staging2
ORDER BY 1;

select*
from layoffs_staging2
WHERE industry LIKE 'CRYPTO%';

UPDATE layoffs_staging2
SET industry = 'crypto'
WHERE industry LIKE 'CRYPTO%';


select*
from layoffs_staging2
WHERE industry LIKE 'CRYPTO%';

select industry
from layoffs_staging2;

select distinct industry
from layoffs_staging2;

select distinct country
from layoffs_staging2
ORDER BY 1;

select  country, TRIM(TRAILING '.' FROM country)
from layoffs_staging2;

UPDATE  layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'united states%';


select distinct country
from layoffs_staging2
ORDER BY 1;

SELECT `date`
from layoffs_staging2;

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT*
from layoffs_staging2;

-- changing the date text  to date --

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- Null/Blank Values--

SELECT*
from layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL ;

SELECT*
from layoffs_staging2 
WHERE industry IS NULL
OR industry = '' ;

SELECT *
FROM layoffs_staging2
WHERE company = 'airbnb' ; 

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '' ;

SELECT t1.industry , t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL ; 

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry 
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL ; 

SELECT *
FROM layoffs_staging2
WHERE company = 'airbnb' ; 

 
-- Remove Unnecessary Columns/Rows--

SELECT*
from layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL ;

DELETE
from layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL ;

SELECT*
from layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Analyzing the data -- 

SELECT*
FROM layoffs_staging2;

SELECT MAX(total_laid_off), max(percentage_laid_off)
FROM layoffs_staging2;

SELECT*
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT*
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT*
FROM layoffs_staging2
WHERE company = 'microsoft';

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 desc;

SELECT MIN(`date`), Max(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 desc;


SELECT*
FROM layoffs_staging2;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 desc ;

SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 desc; 

SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 2 desc; 

SELECT `date`, SUM(total_laid_off), company
FROM layoffs_staging2
GROUP BY `date`, company
ORDER BY 1 desc;

SELECT  YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 desc;

SELECT  stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 1 desc;

SELECT  stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 desc;

-- rolling total layoffs -- 

SELECT substring(`date`, 1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE substring(`date`, 1,7) IS NOT NULL
GROUP BY substring(`date`, 1,7) 
ORDER BY 1 asc;

WITH rolling_total AS
(
SELECT substring(`date`, 1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE substring(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 asc
)
SELECT `MONTH` , total_off, SUM(total_off) OVER(ORDER BY `month`) AS rolling_total
FROM rolling_total ;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 desc;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 asc;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH company_year (company, years , total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), 
Company_year_rank AS
(
SELECT *,
 dense_rank() OVER(partition by YEARS ORDER BY total_laid_off desc) AS RANKING
FROM company_year
WHERE YEARS IS NOT NULL
)
select *
FROM company_year_rank
WHERE ranking <=5 ;