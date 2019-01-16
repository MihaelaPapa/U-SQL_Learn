//Query 1 -- Aggregation 
@result =
	SELECT 
		SUM(salary) as TotalSalary
	FROM @employees;

//Query 2 -- Grouping and aggregation 
@result =
	SELECT 
		DeptName,
		SUM(salary) as SalaryByDept
	FROM @employees
	GROUP BY DeptName;

//Query 3. Window Function 1.
@result=
	SELECT 
		EmpName,
		SUM(Salary) OVER( ) AS SalaryAllDepts 
	FROM @employees;

//Query 4. Window Function 2.
@result=
	SELECT 
		EmpName,
		DeptName
		SUM(Salary) OVER( PARTITION BY DeptName) AS SalaryByDept 
	FROM @employees;
//Query 5. ROW_NUMBER, RANK, DENSE_RANK
@result = 
	SELECT 
		*, //never use this in production!
		ROW_NUMBER() OVER (PARTITION BY Vertical ORDER BY Latency) AS RowNumber,
		RANK()       OVER (PARTITION BY Vertical ORDER BY Latency) AS Rank,
		DENSE_RANK() OVER (PARTITION BY Vertical ORDER BY Latency) AS DenseRank
	FROM @querylog;

//Query 5. NTILE
@result = 
	SELECT 
		*, //never-ever use this in production!
		NTILE(4) OVER(PARTITION BY Vertical ORDER BY Latency) AS Quantile 
	FROM @querylog;

@result = 
	SELECT 
		*, //never-ever use this in production!
		NTILE(4) OVER(PARTITION BY Vertical ORDER BY Vertical) AS Quantile 
	FROM @querylog;

Query 6. TOP N with ROW_NUMBER
@result  =
	SELECT 
		*, //never use this in production!
		ROW_NUMBER() OVER (PARTITION BY Vertical ORDER BY Latency) AS RowNumber
	FROM @querylog;

@result = 
	SELECT * //not in production
	FROM @result 
	WHERE RowNumber <= 3;

Query 7. Assign a unique number to each row using ROW_NUMBER
@result =
	SELECT 
		*, //not in production
		ROW_NUMBER() OVER () AS RowNumber
	FROM @querylog;

//Other interesting functions: CUME_DIST, PERCENT_RANK, PERCENTILE_CONT, PERCENTILE_RANK

///UDFs == .NET methods 

Query 8. Simple UDF | .Net method
//definition
namespace Contoso
{
	public static class Helpers 
	{
		public static string Normalize(string s) 
		{
			s = s.Trim();
			s = s.ToUpper();
			return s;
		}
	}
}
		
//usage
@t = 
	SELECT * FROM 
		(VALUES 
			("2016/03/31", "1:00", "mrys", "@saveenr great demo yesterday", 7)
			("2016/03/31, "7:00", "saveenr", "@mrys Thanks U-SQL RuL3Z!", 4)
		) AS 
			D(date, time, author, tweet, retweets);
@results = 
	SELECT ss
		author,
		Contoso.Helpers.Normalize(author) AS normalized-author
	FROM @t;

OUTPUT @results 
	TO "/output.csv"
	USING Outputters.Csv();

//Query 9. Image recognition.
REFERENCE ASSEMBLY ImageCommon;
REFERENCE ASSEMBLY FaceSdk;
REFERENCE ASSEMBLY ImageEmotion;
REFERENCE ASSEMBLY ImageTagging;
REFERENCE ASSEMBLY InageOcr;

@imgs = 
	EXTRACT FileName string, ImgData byte[]
	FROM @"/images/{FileName:*}.jpg"
	USING new Cognition.Vision.ImageExtractor();

//Extract the number of objects on each image and tag them
@objects = 
	PROCESS @imgs
	PRODUCE FileName,
		NumObjects int,
		Tags string
	READONLY FileName 
	USING new Cognition.Vision.ImageTagger();

OUTPUT @objects
	TO "/objects.tsv"
	USING Outputters.Tsv();

//Query 10. text Analysis
REFERENCE ASSEMBLY [TextCommon];
REFERENCE ASSEMBLY [TextSentiment];
REFERENCE ASSEMBLY [TextKeyPhrase];

@WarAndPeace = 
	EXTRACT No int,
		Year string,
		Book string,
		Chapter string,
		Text string
	FROM @"/usqlext/samples/cognition/war_and_peace.csv"
	USING Extractors.csv();

@sentiment = 
	PROCESS @WarAndPeace
	PRODUCE No,
		Year,
		Book,
		Chapter,
		Text,
		Sentiment string,
		Conf double
	USING new Cognition.Text.SentimentAnalyzer(true);

OUTPUT @sentiment
	TO "/sentiment.tsv"
	USING Outputters.Tsv();
	


