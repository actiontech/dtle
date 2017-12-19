datemath
========

Date math evaluator. [godoc](http://godoc.org/github.com/lytics/datemath)

Based on ElasticSearch's date math:

* [date math docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#date-math)
* [code](https://github.com/elasticsearch/elasticsearch/blob/master/src/main/java/org/elasticsearch/common/joda/DateMathParser.java)

Syntax
------

```
[ operator ] [ number ] [ unit ]

# examples

now-3d   #  now minus 3 days
now+3h   #  now plus 3 hours
+3M      #  now + 3 months, note now is optional
+1y      #  now + 1 year

```
 ``operator`` is either `+` or `-`. ``number`` must be an integer. The units
supported are:

* ``y`` (year)
* ``M`` (month)
* ``w`` (week)
* ``d`` (date)
* ``h`` (hour)
* ``m`` (minute)
* ``s`` (second)
