select *
from covidanalysis.dbo.['coviddeaths']
order by 3,4

select *
from covidanalysis.dbo.['covidvac']
order by 3,4

select location, date, total_cases, new_cases, total_deaths, population
from covidanalysis.dbo.['coviddeaths']
order by 1,2

UPDATE covidanalysis.dbo.['coviddeaths']  
SET total_cases = CONVERT(BIGINT, total_cases)  

UPDATE covidanalysis.dbo.['coviddeaths']  
SET total_deaths = CONVERT(BIGINT, total_deaths)  

--likelyhood of dying if getting covid

select location, date, total_cases, total_deaths, (cast(total_deaths as float)/cast (total_cases as float))*100 as deathpercentage
from covidanalysis.dbo.['coviddeaths']
where location = 'India'
order by 1,2

--total cases vs population

select location, date, total_cases, population, (cast(total_cases as float)/cast (population as float))*100 as infectionpercentage
from covidanalysis.dbo.['coviddeaths']
where location = 'India'
order by 1,2

--looking at countries with the highest infection rate vs population

select location, population, max(cast (total_cases as int)) as maxinfectioncount, max((total_cases/population))*100 as highestinfection
from covidanalysis.dbo.['coviddeaths']
group by location, population
order by highestinfection desc

--countries with the highest death

select location, max(cast (total_deaths as int)) as totaldeathcount
from covidanalysis.dbo.['coviddeaths']
where continent is not null
group by location
order by totaldeathcount desc

--continents with the highest death

select location, max(cast(total_deaths as int)) as totaldeathcount
from covidanalysis.dbo.['coviddeaths']
where continent is null
group by location
order by totaldeathcount desc

select continent, max(cast(total_deaths as int)) as totaldeathcount
from covidanalysis.dbo.['coviddeaths']
where continent is not null
group by continent
order by totaldeathcount desc

select continent, max(cast(total_deaths as int)) as totaldeathcount
from covidanalysis.dbo.['coviddeaths']
where continent is not null
group by continent
order by totaldeathcount desc

select date, max(cast(total_deaths as int)) as totaldeathcount
from covidanalysis.dbo.['coviddeaths']
where continent is not null
group by date
order by totaldeathcount desc

SELECT date, 
       SUM(CAST(new_cases AS INT)) AS totalnewcasescount, 
       SUM(CAST(new_deaths AS INT)) AS totalnewdeathcount, 
       CASE WHEN SUM(CAST(new_cases AS INT)) > 0 
            THEN (SUM(CAST(new_deaths AS INT)) / SUM(CAST(new_cases AS INT))) * 100 
            ELSE 0 
       END AS totaldeathper
FROM covidanalysis.dbo.['coviddeaths']
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY totalnewcasescount DESC

--global deaths in all
select sum(cast(new_cases as int)) as totalnewcasescount, sum(cast(new_deaths as int)) as totalnewdeathcount, 
		(sum(cast(new_deaths as float)) / sum(cast(new_cases as float))) * 100 as totaldeathpercentage
from covidanalysis.dbo.['coviddeaths']
where continent is not null
order by totaldeathpercentage

--joining two tables

select *
from covidanalysis.dbo.['coviddeaths'] as d
join covidanalysis.dbo.['covidvac'] as v
	on d.location = v.location
	and d.date = v.date


--total covid population vs vacinations
select d.continent, d.location, d.date, d.population, v.new_vaccinations
from covidanalysis.dbo.['coviddeaths'] as d
join covidanalysis.dbo.['coviddeaths'] as v
	on d.location = v.location
	and d.date = v.date
where d.continent is not null
order by 2,3

--rolling count and partitioned

select d.continent, d.location, d.date, d.population, v.new_vaccinations, sum(cast(v.new_vaccinations as bigint)) over (partition by d.location order by d.location, d.date)
from covidanalysis.dbo.['coviddeaths'] as d
join covidanalysis.dbo.['coviddeaths'] as v
	on d.location = v.location
	and d.date = v.date
where d.continent is not null and v.new_vaccinations is not null
order by 2,3 

--The reason why the result output is not incremental is that the SUM window function is missing the ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW clause. This means that the SUM function is currently summing all previous rows, not just the current and previous rows. To fix this, you can modify the SUM function to include the ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW clause, like this:
select d.continent, d.location, d.date, d.population, v.new_vaccinations, 
       sum(cast(v.new_vaccinations as bigint)) over (partition by d.location 
                                                     order by d.location, d.date 
                                                     rows between unbounded preceding 
                                                     and current row) as total_vaccinations
from covidanalysis.dbo.['coviddeaths'] as d
join covidanalysis.dbo.['coviddeaths'] as v
	on d.location = v.location
	and d.date = v.date
where d.continent is not null 
order by 2,3


with popvsvac (continent, location, date, population, new_vaccinations, total_vaccinations)
as (
select d.continent, d.location, d.date, d.population, v.new_vaccinations, 
       sum(cast(v.new_vaccinations as bigint)) over (partition by d.location 
                                                     order by d.location, d.date 
                                                     rows between unbounded preceding 
                                                     and current row) as total_vaccinations
from covidanalysis.dbo.['coviddeaths'] as d
join covidanalysis.dbo.['coviddeaths'] as v
	on d.location = v.location
	and d.date = v.date
where d.continent is not null 
)
select *, (total_vaccinations/population)*100 as vaccinationpercentage
from popvsvac

--creating view for later visualization

create view vaccinationpercentage
as
select d.continent, d.location, d.date, d.population, v.new_vaccinations, 
       sum(cast(v.new_vaccinations as bigint)) over (partition by d.location 
                                                     order by d.location, d.date 
                                                     rows between unbounded preceding 
                                                     and current row) as total_vaccinations
from covidanalysis.dbo.['coviddeaths'] as d
join covidanalysis.dbo.['coviddeaths'] as v
	on d.location = v.location
	and d.date = v.date
where d.continent is not null
