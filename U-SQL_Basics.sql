//below queries follow the U-SQL tutorial from mva.com

//Query 1.
//uppercase is mandatory because of the C# integration 
@rows = 
	EXTRACT 
		name string,
		id int
	FROM "/data.csv"
	USING Extractors.Csv();

OUTPUT @rows 
	TO "/output.csv"
	USING Outputters.Csv();

//Query 2.
//Read with or without the header
@rows =
	EXTRACT 
		Name string,
		Id int
	FROM "/file.tsv" 
	USING Extractors.Tsv(skipFirstNRows:1); --specify the numbers of rows to skip

OUTPUT @data 
	TO "/output/docsamples/output_header.csv"
	USING Outputters.Csv(outputHeader:true); --write the header row

//Query 3. File as parameter
DECLARE @inputfile string = "adl://.../data..csv" --file path

@rows =
	EXTRACT <schema>
	FROM @inputfile 
	USING Outputters.Csv();

//Query 4. Declare statements
DECLARE @a string = "Hello World";
DECLARE @b int = 2;
DECLARE @c dateTime = System.DateTime.Parse("1979/03/31");
DECLARE @d dateTime = DateTime.Now;
DECLARE @f byte = new byte[] {0, 1, 2, 3, 4};

/Query 5. It is posible to select/read from values.
@departments =
	SELECT * FROM 
		(VALUES
			(31, "Sales")
			(33, "Engineering")
			(34, "Clerical")
			(35, "Marketing")
		) AS
			D(DepID, DepName);
		
//Query 6. It is possible to refine results in several steps:
@output =
	SELECT 
		Start,
		Region,
		Duration
	FROM @searchlog;

@output = 
	SELECT * 
	FROM @output 
	WHERE Region == "en-gb"  --equality operator is taken from C#

//Query 7. Logical operators.
@ouput =
	SELECT Start, Region, Duration
	FROM @searchlog
	WHERE (Duration >= 60) OR NOT (Region == "en-gb");

//it is possible to have short-circuit evaluation in U-SQL if we write the condition like in the below query
@output =
	SELECT Start, Region, Duration
	FROM @searchlog
	WHERE (Duration >= 60) || !(Region == "en-gb");

//Query 8. Filtering on Dates.
@output = 
	SELECT Start, Region, Duration
	FROM @searchlog
	WHERE 
		Start >= DateTime.Parse("2012/02/16")
		AND Start <= DateTime.Parse("2012/02/17");
//Query 9. IN operator
//not possible to use a subquery with the IN operator in U-QSL
rs = 
	SELECT
		FirstName,
		Lastname,
		JobTitle
	FROM People
	WHERE 
		JobTitle IN ("Engineer","Designer","Writer");

//Query 10. Casting Types

@output=
	SELECT 
		Start,
		Region,
		((double) Duration) AS DurationDouble
	FROM @searchlog;


//Query 11. Filtering on Calculated Columns
//option 1:
@output = 
	SELECT Start,
		Region,
		Duration/60.0 AS DurationInMinutes
	FROM @searchlog
	WHERE Duration/60.0 >= 20;

//option 2:
@output = 
	SELECT Start,
		Region,
		Duration/60.0 AS DurationInMinutes
	FROM @searchlog

@output =
	SELECT *
	FROM @output 
	WHERE DurationInMinutes >= 20;

//Query 12. Sorting
//In U-SQL, unlike in T-SQL, ORDER BY requires a FETCH statement when doin a SELECT.
@output =
	SELECT *
	FROM @searchlog
	ORDER BY Duration ASC
	FETCH FIRST 3 ROWS;
//If we are not doing a SELECT, but writing to a file, then it is possible to have an ORDER BY without a FETCH clause.
OUTPUT @output 
	TO @"/path/file.csv"
	ORDER BY Duration ASC
	USING Outputters.Tsv();

//Query 13. Tables.
// Every table must have an index.
CREATE TABLE Customes(
	OrderId int,
	Customer string,
	Date DteTime,
	Amount float,
	INDEX index1
		CLUSTERED (Customer)
		//Within each distribution keep together rows that have the same value for Customer column
		PARTITIONED BY (Date)
		//physically partition into files by Date column
		DISTRIBUTED BY HASH (id) INTO 4
		//distribute rows into 4 buckets (or less) inside each file based on the HASH of Id 
	);
	
	INSERT INTO Customers
		SELECT * FROM @rows;

//Query 14. Table-Valued Functions
//OPTION 1:
CREATE FUNCTION MyDB.bdo.GetData()
RETURNS @rows TABLE
(
	Name string,
	Id int
)
AS 
BEGIN 
	@rows = 
		EXTRACT 
			Name string,
			Id int
		FROM "/file.tsv"
		USING Extractors.Tsv();
		RETURN;
END;

//OPTION 2:
	CREATE FUNCTION MyDB.dbo.GetData()
	RETURNS @rows AS
	BEGIN 
		@rows =
			EXTRACT 
				Name string,
				Id int
			FROM "/file.tsv"
			USING Extractors.Tsv();
			RETURN;
	END;

//Query 15. Using C# Code
CREATE ASSEMBLY MyCode
	FROM @"/DLLs/Helpers.dll";

REFRENCE ASSEMBLY MyCode;
	@rows =
		SELECT 
			OrdersBD.Helpers.Normalize(Customer) AS CustN,
			Amount AS Amount
		FROM @orders;
	
//Query 16. Grouping and Aggregation.
//Group by is not mandatory in U-SQL (!!!)
//So, the below works:
@output =
	SELECT 
		SUM(Duration) AS TotalDuration 
	FROM @searchlog;

//But the GROUP BY clause is still available:
@output = 
	SELECT 
		Region,
		SUM(Duration) AS TotalDuration 
	FROM searchlog
	GROUP BY Region;

//Query 17. Grouping and Aggregation.
@output = 
     SELECT 
	Region,
	COUNT() AS NumSessions,
	SUM(Duration) AS TotalDuration,
	AVG(Duration) AS AvgDwellTime,
	MAX(Duration) AS MaxDuration,
	MIN(Duration) AS MinDuration
     FROM @searchlog
     GROUP BY Region;

//Query 18. Grouping and Aggregation filtering
//Option1:
@output =
	SELECT 
		Region,
		SUM(Duration) AS TotalDuration
	FROM @searchlog
	GROUP BY Region;
@output2 = 
	SELECT * 
	FROM @output 
	WHERE TotalDuration >200;

//Option2:
@output =
	SELECT 
		Region,
		SUM(Duration) AS TotalDuration
	FROM @searchlog
	GROUP BY Region 
	HAVING sum(Duration) > 200;
	
//Query 19. Read from file and keep filename in result
@rs = 
	EXTRACT 
		user string,
		id string,
		suffix string
	FROM 
		"/input/{suffix}"
	USING Extractors.Csv();	

//Query 20. Read from folder structure containing dates
@rs = 
	EXTRACT 
		user string,
		id string,
		date dateTime
	FROM 
		"/input/{date:yyyy}/{date:MM}/{date:dd}/dat.txt"
	USING Extractors.Csv();	

//Query 21. It is possible to combine Query 19 and Query 20.
@rs =
	EXTRACT 
		user string,
		id string,
		date DateTime,
		suffix string
	FROM 
		"/input/{date:yyyy}/{date:MM}/{date:dd}/{sufix}"
	USING Extractors.Csv();

@rs = 
	SELECT * FROM @rs
	WHERE date >= System.DateTime.Parse("2016/1/1")
		AND date < System.DateTime.Parse("2016/2/1");


	




