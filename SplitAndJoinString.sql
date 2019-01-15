@a = SELECT region, Urls FROM @searchlog

@b = SELECT Region, SqlArray.Create(Urls.Split(';')) as UrlTokens
	FROM @a

@c = 	SELECT 
		Region,
		Token AS Url
     	FROM @b
	CROSS APPLY EXPLODE (UrlTokens) AS r(Token);

@d = SELECT     Region,
		ARRAY_AGG<string>(Url).ToArray() as UrlArray
	FROM @c
	GROUP BY Region;

@e = SELECT Region,
	string.Join(";", UrlArray) as Urls
	FROM @c;