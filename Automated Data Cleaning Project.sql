-- Automated Data Cleaning Project 
-- Create a stored procedure that automatically cleans the data
-- Within the procedure, create a duplicate table of the dataset being used and then create an event 
select * from ushouseholdincome;

SELECT * FROM bakery.ushouseholdincomecleaned;

delimiter $$
drop procedure if exists Copy_and_Clean_Data;
create procedure Copy_and_Clean_Data()
begin
	-- CREATING OUR TABLE
	CREATE TABLE IF NOT EXISTS `ushouseholdincomecleaned` (
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
	  `Area_Code` varchar(10) DEFAULT NULL,
	  `ALand` bigint DEFAULT NULL,
	  `AWater` bigint DEFAULT NULL,
	  `Lat` double DEFAULT NULL,
	  `Lon` double DEFAULT NULL,
	  `TimeStamp` timestamp DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    
	-- COPY DATA TO NEW TABLE
	INSERT INTO ushouseholdincomecleaned
    SELECT *, current_timestamp()
    FROM ushouseholdincome;
    
    -- DATA CLEANING STEPS
	-- Remove Duplicates
	DELETE FROM ushouseholdincomecleaned 
	WHERE 
		row_id IN (
		SELECT row_id
	FROM (
		SELECT row_id, id,
			ROW_NUMBER() OVER (
				PARTITION BY id, `TimeStamp`
				ORDER BY id, `TimeStamp`) AS row_num
		FROM 
			ushouseholdincomecleaned
	) duplicates
	WHERE 
		row_num > 1
	);

	-- Fixing some data quality issues by fixing typos and general standardization
	UPDATE ushouseholdincomecleaned
	SET State_Name = 'Georgia'
	WHERE State_Name = 'georia';

	UPDATE ushouseholdincomecleaned
	SET County = UPPER(County);

	UPDATE ushouseholdincomecleaned
	SET City = UPPER(City);

	UPDATE ushouseholdincomecleaned
	SET Place = UPPER(Place);

	UPDATE ushouseholdincomecleaned
	SET State_Name = UPPER(State_Name);

	UPDATE ushouseholdincomecleaned
	SET `Type` = 'CDP'
	WHERE `Type` = 'CPD';

	UPDATE ushouseholdincomecleaned
	SET `Type` = 'Borough'
	WHERE `Type` = 'Boroughs';

    
end $$
delimiter ;

CALL Copy_and_Clean_Data();

-- CREATE EVENT
drop event if exists run_data_cleaning;
create event run_data_cleaning
	on schedule every 30 day
    do call Copy_and_Clean_Data();

-- CREATE TRIGGER
delimiter $$
create trigger transfer_clean_data
	after insert on ushouseholdincome
    for each row
begin
	CALL Copy_and_Clean_Data();
end $$
delimiter ;





















-- DEBUGGING OR CHECKING SP WORKS
		SELECT row_id, id, row_num
    FROM (
		SELECT row_id, id,
			ROW_NUMBER() OVER (
				PARTITION BY id
				ORDER BY id) AS row_num
		FROM 
			ushouseholdincomecleaned
	) duplicates
	WHERE 
		row_num > 1;

SELECT COUNT(row_id)
from ushouseholdincomecleaned;

SELECT State_Name, count(State_Name)
from ushouseholdincomecleaned
group by State_Name;
 
SELECT distinct TimeStamp FROM bakery.ushouseholdincomecleaned;
