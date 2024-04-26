create database world_layoffs;
use world_layoffs;
select * from layoffs;

-- 1. remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove any columns

create table layoffs_stagging
like layoffs;

select * from layoffs_stagging;

insert layoffs_stagging
select * from layoffs;

SELECT 
    *
FROM
    layoffs;
    
select *,row_number() over(partition by company,industry,total_laid_off
,percentage_laid_off,'date') as row_num from layoffs_stagging;

with duplicate_cte as 
(select *,row_number() over(partition by company,location,industry,total_laid_off
,percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num from layoffs_stagging)
select * from duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_stagging2` (
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


select * from layoffs_stagging2
where row_num > 1;

insert into layoffs_stagging2
select *, row_number() over(partition by company,location,industry,total_laid_off
,percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
from layoffs_stagging;

delete 
from layoffs_stagging2
where row_num > 1;


select *
from layoffs_stagging2
where row_num > 1;

select *
from layoffs_stagging2;

SET SQL_SAFE_UPDATES = 0;

-- standardizing data
select company,trim(company)
from layoffs_stagging2;

update layoffs_stagging2 set company = trim(company);

select distinct industry
from layoffs_stagging2
order by 1;

select * 
from layoffs_stagging2
where industry like 'Crypto%';

update layoffs_stagging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry
from layoffs_stagging2;

select * 
from layoffs_stagging2;

select distinct location
from layoffs_stagging2
order by 1;


select distinct country
from layoffs_stagging2
order by 1;

select *
from layoffs_stagging2
where country like 'United States%'
order by 1;

select distinct country,trim(trailing '.' from country)
from layoffs_stagging2
order by 2; 

update layoffs_stagging2
set country = trim(trailing '.' from country)
where country like 'United State%';

select *
from layoffs_stagging2;

select `date`
from layoffs_stagging2;

alter table layoffs_stagging2
modify column `date` date;

SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_stagging2 ;

update layoffs_stagging2 
set `date` = str_to_date(`date`,'%m/%d/%Y');

select *
from layoffs_stagging2
where total_laid_off is 
null and percentage_laid_off is null;

select *
from layoffs_stagging2
where industry is null
or industry = '';

select *
from layoffs_stagging2
where company = 'Airbnb';

select * from layoffs_stagging2 t1
join layoffs_stagging2 t2
on t1.company = t2.company
and t1.location = t2.location
where t1.industry is null and t2.industry is not null;

select t1.industry,t2.industry from layoffs_stagging2 t1
join layoffs_stagging2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;


update layoffs_stagging2 t1
join layoffs_stagging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

update layoffs_stagging2
set industry = null
where industry = '';


select *
from layoffs_stagging2
where company like 'Bally%';

select *
from layoffs_stagging2;

select *
from layoffs_stagging2
where total_laid_off is null
and percentage_laid_off is null;

delete from layoffs_stagging2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoffs_stagging2;

alter table layoffs_stagging2
drop column row_num;



-- Exploratory Data Analysis

select * from layoffs_stagging2;

select max(total_laid_off),max(percentage_laid_off) from layoffs_stagging2;

select * from layoffs_stagging2
where percentage_laid_off = 1
order by total_laid_off desc;

select company,sum(total_laid_off) from layoffs_stagging2
group by company
order by 2 desc;

select min(`date`),max(`date`) from layoffs_stagging2;

select industry,sum(total_laid_off) from layoffs_stagging2
group by industry
order by 2 desc;

select * from layoffs_stagging2;

select country,sum(total_laid_off) from layoffs_stagging2
group by country
order by 2 desc;

select year(`date`),sum(total_laid_off) from layoffs_stagging2
group by year(`date`)
order by 1 desc;

select stage,sum(total_laid_off) from layoffs_stagging2
group by stage
order by 2 desc;

select company,sum(percentage_laid_off) from layoffs_stagging2
group by company
order by 2 desc;

select company,avg(percentage_laid_off) from layoffs_stagging2
group by company
order by 2 desc;

select substring(`date`,1,7) as `MONTH`,sum(total_laid_off) from layoffs_stagging2
where substring(`date`,1,7)
group by `MONTH`
order by 1 Asc;

with Rolling_total as 
(select substring(`date`,1,7) as `MONTH`,sum(total_laid_off) as total_off
 from layoffs_stagging2
where substring(`date`,1,7) is not null
group by `MONTH`
order by 1 Asc)
select `MONTH`,total_off,sum(total_off) over(order by `MONTH`) as rolling_total
from rolling_total;


select company,sum(total_laid_off) from layoffs_stagging2
group by company
order by 2 desc;

select company,year(`date`),sum(total_laid_off) from layoffs_stagging2
group by company,year(`date`)
order by 3 desc;


with company_year (compnay,years,total_laid_off) as
(select company,year(`date`),sum(total_laid_off) from layoffs_stagging2
group by company,year(`date`)
),company_year_rank as 
(select *,dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year 
where years is not null
)
select * from company_year_rank
where ranking <= 5 ;

