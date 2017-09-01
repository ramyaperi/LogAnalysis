#!/usr/bin/env python
# "Database code" for the DB news.

import datetime
import psycopg2
#import bleach

DBName = "news"

# Query to get top articles

top_articles_query = """Select articles.title, top_article.num
                   from(
                   select split_part(path,'/',3) as name, count(*) as num
                   from log
                   group by path
                   order by num Desc
                   limit 3 offset 1)top_article join articles on articles.slug=top_article.name order by top_article.num Desc;  """

# Query to get top authors
top_authors_query = """select  authors.name  , sum(read_count.num ) as views
                 from(
                 select split_part(path,'/',3) as name, count(*) as num
                 from log group by path )read_count
                 join articles on articles.slug=read_count.name
                 join authors ON authors.id =articles.author
                 group by authors.name
                 order by views Desc;"""

# Query to get error percent per day
error_days_query = """SELECT * FROM (
               Select totals.date ,(((errors.error)*100.0)/(totals.total)) as errorpercent
               from (
               select time::date as date , count(*) as total
               from log
               group by date) totals join
               (select time::date as date , count(*) as error
               from log
               where status='404 NOT FOUND'
               group by date) errors
               on totals.date=errors.date ) totaltab
               where  errorpercent>1.0;"""

# Function that makes connects to database and extracts data


def getData(Query):
    """Get the data for the resulting query"""
    db = psycopg2.connect(database=DBName)
    c = db.cursor()
    c.execute(Query)
    return c.fetchall()
    db.close()


# Most popular articles of all times
print "The most popular three articles of all time: "
top_articles = getData(top_articles_query)
for i, j in top_articles:
    print "'" + i + "'--", str(j) + " views"
print "\n"
# Most popular author of all times
print "The most popular article authors of all time: "
top_authors = getData(top_authors_query)
for i, j in top_authors:
    print "'" + i + "'--", str(j) + " views"
print "\n"
# which days did more than 1% of requests lead to errors
print "Days with more than 1% of requests leading to errors:"
error_days = getData(error_days_query)
for i, j in error_days:
    print str(i) + "  -- ", str(j) + " %"
print "\n"
