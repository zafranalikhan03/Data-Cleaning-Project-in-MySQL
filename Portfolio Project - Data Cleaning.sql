############################################################# 
############## Data Cleaning With SQL Project ###############
############################################################# 

SELECT * 
FROM world_layoffs.layoffs;

##########################################################################################
# first we want to create a staging table. This will be the one on which we will be work-#
# ing and cleaning the data. We want a Copy of raw data if there is any mishap 			 #
##########################################################################################

CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

##########################################################################################
# 		We will be performing the following steps for data cleaning 					 #
# 		1. Checking for duplicates and Removing if exists								 #
# 		2. Standardizing the data and fixing the errors									 #
# 		3. Looking at null values and seeing what 										 #
# 		4. removing unnecessary column/s and row/s through few ways					 	 #
##########################################################################################


########################### 1. Remove Duplicates  ###########################

## First let's check for duplicates ##

SELECT *
FROM world_layoffs.layoffs_staging
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;



SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- let's just look at oda to confirm
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'
;
-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- these are our real duplicates 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

-- now you may want to write it like this:
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;


WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

####################################################################################
# There is a solution, which could be a good one. Which is to create a new column  #
# and add those row numbers in. Then delete where row numbers are over 2, then 	   #
# delete that column. so let's do it!!											   #
#################################################################################### 
 
ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;

SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

#######################################################################################
#     now after we have this we can delete rows were row_num is greater than 2        #
#######################################################################################

DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;


################################   2. Standardize Data  ################################ 

SELECT * 
FROM world_layoffs.layoffs_staging2;

################################################################################# 
#	By looking at industry it looks like we have some null and empty rows, 		#
#	let's take a look at these. 												#
################################################################################# 
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

#################### Take a look at these ####################
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

#################### Nothing is wrong here ####################
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

########################################################################################### 
#	It looks like airbnb is a travel, but this one just isn't populated. 				  #
#	I'm sure it's same for the others. What we can do is:								  #
#	writing a query that if there is another row with the same company name, it 		  #
#	will update it to the non-null industry values 										  #
#	makes it easy so if there were thousands we wouldn't have to manually check them all  #
###########################################################################################

##### We should set the blanks to nulls since those are typically easier to work with #####

UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

############ Now if we check those are all null ############

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

############ Now we need to populate those nulls if possible ############

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

############ And if we check it looks like Bally's was the 					############ 
############ only one without a populated row to populate this null values  ############ 
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

############ ############ ############ ############ ############ ############ ############## 
# I also noticed the Crypto has multiple different variations. We need to standardize that #
############ ############ ############ ############ ############ ############ ##############

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');


SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

############ ############ ############ ############ 
# 			we also need to look at this 		  #
############ ############ ############ ############
 
SELECT *
FROM world_layoffs.layoffs_staging2;
############ ############ ############ ############ ############ ############ ############ 
#	Eeverything looks good except apparently we have some "United States" and some 		 #
#	"United States." with a period at the end. Let's standardize this. 					 #
############ ############ ############ ############ ############ ############ ############ 

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

############ ############ ############ ############ ############ 
#		Let's fix the date column's data-type problem: 		   #
############ ############ ############ ############ ############ 

SELECT *
FROM world_layoffs.layoffs_staging2;

############ We will use str_to_date function to update this field ############ 

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

############  now we can convert the data type properly ############ 
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM world_layoffs.layoffs_staging2;


############ ############ ############ 3. Look at Null Values ############ ############ ############ 

########################################################################################################
#	The null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. #
#	I don't think I want to change that. I like having them null because it makes it easier for 	   #
#	calculations during the EDA phase																   #
########################################################################################################

-- so there isn't anything I want to change with the null values
########################################################################
#			 4. remove any columns and rows we need to 				   #
########################################################################

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

############################################################
#		Delete Useless data we can't really use 		   #
############################################################

DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM world_layoffs.layoffs_staging2;
