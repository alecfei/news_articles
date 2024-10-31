DROP TABLE articles, sources, authors, categories, facebookshares

/*
-- create tables according to upcoming data structure
CREATE TABLE articles (
    id TEXT PRIMARY KEY,
    is_duplicate BOOLEAN,
    datetime_found TIMESTAMP,
    datetime_published TIMESTAMP,
    article_type TEXT,
    sim FLOAT,
    url TEXT,
    title TEXT,
    body TEXT,
    image TEXT,
    sentiment FLOAT,
    relevance FLOAT
);

CREATE TABLE sources (
    article_id TEXT,
    source_name TEXT,
    source_link TEXT,
    UNIQUE(article_id, source_name)
);

CREATE TABLE authors (
    article_id TEXT REFERENCES articles(id),
    author_name TEXT,
    author_email TEXT,
    author_type TEXT,
    is_agency BOOLEAN,
    UNIQUE(article_id, author_name, author_email)
);

CREATE TABLE categories (
    article_id TEXT,
    label TEXT,
    keyword_1 TEXT,
    keyword_2 TEXT,
    keyword_3 TEXT,
    UNIQUE(article_id)
);

CREATE TABLE facebookshares (
    article_id TEXT,
    shares INT,
	UNIQUE(article_id)
);
*/

-- check duplicates
SELECT id, COUNT(*) as duplicate_count
FROM public.articles
GROUP BY id
HAVING COUNT(*) > 1;

-- top shares
SELECT a.id, f.shares, a.datetime_published, a.url, a.title, a.sentiment FROM public.facebookshares f
INNER JOIN public.articles a
ON f.article_id = a.id
WHERE f.shares != 'NaN'
ORDER BY f.shares DESC;

