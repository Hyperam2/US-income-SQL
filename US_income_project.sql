select*
from us_household_income;

select*
from us_household_income_statistics;

#ALTER TABLE
ALTER TABLE us_household_income_statistics
RENAME COLUMN `ï»¿id` TO `id`;

#COUNT DATA - Missing 230 Rows

select COUNT(id)
from us_household_income;

select COUNT(id)
from us_household_income_statistics;

#Clean DATA

select id, count(id)
from us_household_income
Group by id
having count(id)>1;

select id, count(id)
from us_household_income_statistics
Group by id
having count(id)>1;


SELECT*
FROM(
Select row_id,
id,
ROW_Number()over(Partition by id order by id) as row_num
from us_household_income
) as duplicates
Where row_num >1;

DELETE FROM us_household_income
WHERE row_id IN(
	SELECT row_id
		FROM (
		Select row_id,
		id,
		ROW_Number()over(Partition by id order by id) as row_num
		from us_household_income
		) as duplicates
Where row_num >1);


Select DISTINCT State_Name, Count(State_Name)
from us_household_income_statistics
group by State_Name;

Update us_household_income
SET state_name = 'Georgia'
where state_name = 'georia';

Update us_household_income
SET state_name = 'Alabama'
where state_name = 'alabama';


Select DISTINCT State_ab
from us_household_income
group by 1;



select *
from us_household_income
where Place=''
order by 1;

Update us_household_income
SET Place = 'Autaugaville'
where Place = '';

Select type, count(type)
from us_household_income
group by type;


Update us_household_income
SET type = 'Borough'
where type = 'Boroughs';

Update us_household_income
SET type = 'CDP'
where type = 'CPD';

UPDATE us_household_income
SET County = UPPER(County);

UPDATE us_household_income
SET City = UPPER(City);

UPDATE us_household_income
SET Place = UPPER(Place);


Select ALand, AWater
From us_household_income
Where (AWater = 0 OR AWater ='' OR AWater is NULL)
AND (ALand = 0 OR ALand ='' OR ALand is NULL)
;




#EXPLOITORY


#AUTOMATIC

DELIMITER $$
Drop procedure if exists Copy_and_Clean_Data;
Create procedure Copy_and_Clean_Data()

BEGIN
 #Create Our Table
	CREATE TABLE IF NOT EXISTS `us_household_income_clean` (
	  `row_id` int DEFAULT NULL,
	  `id` int DEFAULT NULL,
	  `State_Code` int DEFAULT NULL,
	  `State_Name` text,
	  `State_ab` text,
	  `County` text,
	  `City` text,
	  `Place` text,
	  `Type` text,
	  `Primary` text,
	  `Zip_Code` int DEFAULT NULL,
	  `Area_Code` int DEFAULT NULL,
	  `ALand` int DEFAULT NULL,
	  `AWater` int DEFAULT NULL,
	  `Lat` double DEFAULT NULL,
	  `Lon` double DEFAULT NULL,
	  `TimeStamp` TIMESTAMP DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


#Copy our data
INSERT INTO us_household_income_clean
select*, CURRENT_TIMESTAMP
From us_project.us_household_income;


	DELETE FROM us_household_income_clean 
	WHERE 
		row_id IN (
		SELECT row_id
	FROM (
		SELECT row_id, id,
			ROW_NUMBER() OVER (
				PARTITION BY id,`TimeStamp`
				ORDER BY id,`TimeStamp`) AS row_num
		FROM 
			us_household_income_clean
	) duplicates
	WHERE 
		row_num > 1
	);


	UPDATE us_household_income_clean
	SET State_Name = 'Georgia'
	WHERE State_Name = 'georia';

	UPDATE us_household_income_clean
	SET County = UPPER(County);

	UPDATE us_household_income_clean
	SET City = UPPER(City);

	UPDATE us_household_income_clean
	SET Place = UPPER(Place);

	UPDATE us_household_income_clean
	SET State_Name = UPPER(State_Name);

	UPDATE us_household_income_clean
	SET `Type` = 'CDP'
	WHERE `Type` = 'CPD';

	UPDATE us_household_income_clean
	SET `Type` = 'Borough'
	WHERE `Type` = 'Boroughs';

END $$
DELIMITER ;

CALL Copy_and_Clean_Data();


#DEBUG
DELETE FROM us_household_income_clean
WHERE row_id IN(
	SELECT row_id
		FROM (
		Select row_id,
		id,
		ROW_Number()over(Partition by id order by id) as row_num
		from us_household_income
		) as duplicates
Where row_num >1);

select Count(row_id)
From us_household_income_clean;

select Count(row_id)
From us_household_income;


select State_Name, Count(state_name)
From us_household_income_clean
Group by State_name;

select State_Name, Count(state_name)
From us_household_income
Group by State_name;


#Create Event
Drop Event run_data_cleaning;
Create Event run_data_cleaning
	on schedule Every 30 day
    do call Copy_and_Clean_Data();


#Create Trigger

DELIMITER $$
CREATE Trigger Transfer_Clean_Data
	After Insert on us_project.us_household_income_clean
    For each row
    Begin
		Call Copy_and_Clean_Data();
	END $$
DELIMITER ;

# EXPLORITY DATA

Select State_Name,County,City,ALand,AWater
from us_project.us_household_income;


Select State_Name,SUM(ALand),SUM(AWater)
from us_project.us_household_income
group by State_Name
order by 2 DESC 
Limit 10;

Select State_Name,SUM(ALand),SUM(AWater)
from us_project.us_household_income
group by State_Name
order by 3 DESC
Limit 10;

Select *
From  us_project.us_household_income as U
Join  us_project.us_household_income_statistics as US
	ON U.id=us.id;
    
    
Select *
From  us_project.us_household_income as U
Right Join  us_project.us_household_income_statistics as US
	ON U.id=us.id
Where u.id is NULL;


Select *
From  us_project.us_household_income as U
INNER Join  us_project.us_household_income_statistics as US
	ON U.id=us.id
Where Mean <> 0;



Select u.State_Name, ROUND(AVG(Mean),1) as Average_Income, ROUND(AVG(Median),1) as Median_Income
From  us_project.us_household_income as U
INNER Join  us_project.us_household_income_statistics as US
	ON U.id=us.id
Where Mean <> 0
Group by U.State_Name
Order by 2 DESC
Limit 10;

Select `Type`, COUNT(`Type`),ROUND(AVG(Mean),1) as Average_Income, ROUND(AVG(Median),1) as Median_Income
From  us_project.us_household_income as U
INNER Join  us_project.us_household_income_statistics as US
	ON U.id=us.id
Where Mean <> 0
Group by `Type`
Order by 3 DESC
Limit 10;

Select `Type`, COUNT(`Type`),ROUND(AVG(Mean),1) as Average_Income, ROUND(AVG(Median),1) as Median_Income
From  us_project.us_household_income as U
INNER Join  us_project.us_household_income_statistics as US
	ON U.id=us.id
Where Mean <> 0
Group by `Type`
Having COUNT(`Type`) >100
Order by 3 DESC
Limit 20;


Select u.state_name, City, Round(AVG(Mean),1)
From  us_project.us_household_income as U
Join  us_project.us_household_income_statistics as US
	ON U.id=us.id
Group by u.state_name, City
Order by 3 DESC;
    