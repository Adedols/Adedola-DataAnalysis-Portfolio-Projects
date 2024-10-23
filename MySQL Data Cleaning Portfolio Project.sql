-- DATA CLEANING

-- Things to look out for when cleaning data
-- 1. Remove duplicates if any
-- 2. Standardize the data
-- 3. Null or blank Values
-- 4. Remove unecessary columns or rows


-- This is the raw dataset
select *
from world_layoffs.layoffs;

-- It's best practice not to make changes on the original raw dataset
-- Instead we can create a staging dataset and clean that
create table layoffs_staging
like world_layoffs.layoffs;

select *
from world_layoffs.layoffs_staging;

insert world_layoffs.layoffs_staging
select *
from world_layoffs.layoffs;

-- 1. Removing duplicates
select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from world_layoffs.layoffs_staging; 

-- Creating a CTE to identify duplicate rows
with duplicate_cte as
(
select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) as row_num
from world_layoffs.layoffs_staging
)
select *
from duplicate_cte
where row_num > 1; 

select *
from layoffs_staging
where company = 'Yahoo'; 

-- Creating another staging table and including the row numbers as a column
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

select *
from world_layoffs.layoffs_staging2;

insert into world_layoffs.layoffs_staging2
select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) as row_num
from world_layoffs.layoffs_staging; 


-- From layoffs_staging 2, we can delete the deuplicates
delete
from world_layoffs.layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2;

-- Standardize the data: Finding issues in your data and fixing it
-- (a) Removing the space from the company column
select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

-- (b) Correcting the spelling of 'crypto' in the industry column
select distinct industry
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- (c) Removing the '.' after United States
select distinct country, trim(trailing '.' from country)
from layoffs_staging2
where country like 'United States%'
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- (d) Changing the data type  and format of the 'date' column from text to date dtype and m/d/Y
select `date`,
str_to_date(`date`, '%m/%d/%Y') 
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2; 

-- Working with Null and Blank values
select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null; 

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company like 'Bally%';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null)
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null)
and t2.industry is not null;

select *
from layoffs_staging2;


-- Removing unecessary rows or columns
select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;
