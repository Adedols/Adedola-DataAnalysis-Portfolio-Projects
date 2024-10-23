-- EXPLORATORY DATA ANALYSIS

-- Let's take a look at our cleaned dataset
select *
from layoffs_staging2;

-- Looking at the max number of employees that were layed off and the max pecentage of layoffs
select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

-- Total number of layoffs by company from the largest to the smallest
select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

-- Total number of layoffs by industry from the largest to the smallest
select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

-- Total number of layoffs by country from the largest to the smallest
select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

-- The earliest and last date that a layoff took place
select min(`date`), max(`date`)
from layoffs_staging2;

select *
from layoffs_staging2; 

-- Total number of layoffs per year
select year(`date`) date_to_year, sum(total_laid_off) as sum_total_laid_off
from layoffs_staging2
where year(`date`) is not null
group by year(`date`)
order by 1 desc;

-- The number of layoffs per company stage; Post-IPO, Acquired, Series A etc
select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

-- Layoff information of companies with the highest percentage layoffs in order of the funds 
-- raised by these companies
select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

-- Average percentage laid off by company
select company, avg(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

-- Average percentage laid off by country
select country, avg(percentage_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

-- Average percentage laid off by industry
select industry, avg(percentage_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

-- Rolling total layoffs by Date
select SUBSTRING(`date`, 1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where SUBSTRING(`date`, 1,7) is not null
group by `month`
order by 1 asc;

-- Putting the above query into a Common Table Expression and executing a query 
-- with a window function
with Rolling_Total as
(
select SUBSTRING(`date`, 1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where SUBSTRING(`date`, 1,7) is not null
group by `month`
order by 1 asc
)
select `month`, total_off,
sum(total_off) over(order by `month`) as rolling_total
from Rolling_Total;

select *
from layoffs_staging2;

-- Total layoffs of companies by year in descending order and dense ranked
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;

-- Combining two CTEs; the first containing the query above and 
-- the second dense ranking the contents of the first CTE and then querying off of the second CTE
with Company_Year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
), Company_Year_Rank as
(
select *, 
dense_rank() over(partition by years order by total_laid_off desc) as Ranking
from Company_Year 
where years is not null
)
select *
from Company_Year_Rank
where Ranking <= 5;


-- Total layoffs of companies by country and location in descending order and dense ranked
select company, country, location, sum(total_laid_off)
from layoffs_staging2
group by company, country, location
order by 4 desc;

-- Combining two CTEs; the first containing the query above and 
-- the second dense ranking the contents of the first CTE and then querying off of the second CTE
with Company_Location (company, country, location, total_laid_off) as
(
select company, country, location, sum(total_laid_off)
from layoffs_staging2
group by company, country, location
), Company_Location_Rank as
(
select *, 
dense_rank() over(partition by country, location order by total_laid_off desc) as Ranking
from Company_Location 
where location is not null
) 
select *
from Company_Location_Rank
where Ranking <= 5;

