
=============
Optimizations
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Introduction
============

In this doc, we will cover the optimization solution in the query engine and the limitations.

Logical Plan Optimization
=========================

The Logical Plan optimization transfer the logical operator by predefined rules. The following section will cover the detail rules.

Where Clause Optimization
-------------------------
The where clause will reduce the size of rows markedly, thus the where clause optimization is the most important optimization.

There are two rules involved in the core engine.

Filter Merge Rule
-----------------

The consecutive Filter operator will be merged as one Filter operator::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_ppl/_explain \
    ... -d '{"query" : "source=accounts | where age > 10 | where age < 20 | fields age"}'
    {
      "root": {
        "name": "ProjectOperator",
        "description": {
          "fields": "[age]"
        },
        "children": [
          {
            "name": "OpenSearchIndexScan",
            "description": {
              "request": "OpenSearchQueryRequest(indexName=accounts, sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"query\":{\"bool\":{\"filter\":[{\"range\":{\"age\":{\"from\":null,\"to\":20,\"include_lower\":true,\"include_upper\":false,\"boost\":1.0}}},{\"range\":{\"age\":{\"from\":10,\"to\":null,\"include_lower\":false,\"include_upper\":true,\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"_source\":{\"includes\":[\"age\"],\"excludes\":[]},\"sort\":[{\"_doc\":{\"order\":\"asc\"}}]}, searchDone=false)"
            },
            "children": []
          }
        ]
      }
    }


Filter Push Down Under Sort
---------------------------

The Filter operator should be push down under Sort operator::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_ppl/_explain \
    ... -d '{"query" : "source=accounts | sort age | where age < 20 | fields age"}'
    {
      "root": {
        "name": "ProjectOperator",
        "description": {
          "fields": "[age]"
        },
        "children": [
          {
            "name": "OpenSearchIndexScan",
            "description": {
              "request": "OpenSearchQueryRequest(indexName=accounts, sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"query\":{\"range\":{\"age\":{\"from\":null,\"to\":20,\"include_lower\":true,\"include_upper\":false,\"boost\":1.0}}},\"_source\":{\"includes\":[\"age\"],\"excludes\":[]},\"sort\":[{\"age\":{\"order\":\"asc\",\"missing\":\"_first\"}}]}, searchDone=false)"
            },
            "children": []
          }
        ]
      }
    }


OpenSearch Specific Optimization
================================

The OpenSearch `Query DSL <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html>`_ and `Aggregation <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html>`_ also enabling the storage engine specific optimization.

Push Project Into Query DSL
---------------------------
The Project list will push down to Query DSL to `filter the source <https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-fields.html#source-filtering>`_::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_sql/_explain \
    ... -d '{"query" : "SELECT age FROM accounts"}'
    {
      "root": {
        "name": "ProjectOperator",
        "description": {
          "fields": "[age]"
        },
        "children": [
          {
            "name": "OpenSearchIndexScan",
            "description": {
              "request": "OpenSearchQueryRequest(indexName=accounts, sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"_source\":{\"includes\":[\"age\"],\"excludes\":[]}}, searchDone=false)"
            },
            "children": []
          }
        ]
      }
    }

Filter Merge Into Query DSL
---------------------------

The Filter operator will merge into OpenSearch Query DSL::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_sql/_explain \
    ... -d '{"query" : "SELECT age FROM accounts WHERE age > 30"}'
    {
      "root": {
        "name": "ProjectOperator",
        "description": {
          "fields": "[age]"
        },
        "children": [
          {
            "name": "OpenSearchIndexScan",
            "description": {
              "request": "OpenSearchQueryRequest(indexName=accounts, sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"query\":{\"range\":{\"age\":{\"from\":30,\"to\":null,\"include_lower\":false,\"include_upper\":true,\"boost\":1.0}}},\"_source\":{\"includes\":[\"age\"],\"excludes\":[]},\"sort\":[{\"_doc\":{\"order\":\"asc\"}}]}, searchDone=false)"
            },
            "children": []
          }
        ]
      }
    }

Sort Merge Into Query DSL
-------------------------

The Sort operator will merge into OpenSearch Query DSL::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_sql/_explain \
    ... -d '{"query" : "SELECT age FROM accounts ORDER BY age"}'
    {
      "root": {
        "name": "ProjectOperator",
        "description": {
          "fields": "[age]"
        },
        "children": [
          {
            "name": "OpenSearchIndexScan",
            "description": {
              "request": "OpenSearchQueryRequest(indexName=accounts, sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"_source\":{\"includes\":[\"age\"],\"excludes\":[]},\"sort\":[{\"age\":{\"order\":\"asc\",\"missing\":\"_first\"}}]}, searchDone=false)"
            },
            "children": []
          }
        ]
      }
    }

Because the OpenSearch Script Based Sorting can't handle NULL/MISSING value, there is one exception is that if the sort list include expression other than field reference, it will not be merged into Query DSL::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_sql/_explain \
    ... -d '{"query" : "SELECT age FROM accounts ORDER BY abs(age)"}'
    {
      "root": {
        "name": "ProjectOperator",
        "description": {
          "fields": "[age]"
        },
        "children": [
          {
            "name": "SortOperator",
            "description": {
              "sortList": {
                "abs(age)": {
                  "sortOrder": "ASC",
                  "nullOrder": "NULL_FIRST"
                }
              }
            },
            "children": [
              {
                "name": "OpenSearchIndexScan",
                "description": {
                  "request": "OpenSearchQueryRequest(indexName=accounts, sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\"}, searchDone=false)"
                },
                "children": []
              }
            ]
          }
        ]
      }
    }

Limit Merge Into Query DSL
--------------------------

The Limit operator will merge in OpenSearch Query DSL::

        sh$ curl -sS -H 'Content-Type: application/json' \
        ... -X POST localhost:9200/_plugins/_sql/_explain \
        ... -d '{"query" : "SELECT age FROM accounts LIMIT 10 OFFSET 5"}'
        {
          "root": {
            "name": "ProjectOperator",
            "description": {
              "fields": "[age]"
            },
            "children": [
              {
                "name": "OpenSearchIndexScan",
                "description": {
                  "request": "OpenSearchQueryRequest(indexName=accounts, sourceBuilder={\"from\":5,\"size\":10,\"timeout\":\"1m\",\"_source\":{\"includes\":[\"age\"],\"excludes\":[]}}, searchDone=false)"
                },
                "children": []
              }
            ]
          }
        }

If sort that includes expression, which cannot be merged into query DSL, also exists in the query, the Limit operator will not be merged into query DSL as well::

        sh$ curl -sS -H 'Content-Type: application/json' \
        ... -X POST localhost:9200/_plugins/_sql/_explain \
        ... -d '{"query" : "SELECT age FROM accounts ORDER BY abs(age) LIMIT 10"}'
        {
          "root": {
            "name": "ProjectOperator",
            "description": {
              "fields": "[age]"
            },
            "children": [
              {
                "name": "TakeOrderedOperator",
                "description": {
                  "limit": 10,
                  "offset": 0,
                  "sortList": {
                    "abs(age)": {
                      "sortOrder": "ASC",
                      "nullOrder": "NULL_FIRST"
                    }
                  }
                },
                "children": [
                  {
                    "name": "OpenSearchIndexScan",
                    "description": {
                      "request": "OpenSearchQueryRequest(indexName=accounts, sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\"}, searchDone=false)"
                    },
                    "children": []
                  }
                ]
              }
            ]
          }
        }

Aggregation Merge Into OpenSearch Aggregation
---------------------------------------------

The Aggregation operator will merge into OpenSearch Aggregation::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_sql/_explain \
    ... -d '{"query" : "SELECT gender, avg(age) FROM accounts GROUP BY gender"}'
    {
      "root": {
        "name": "ProjectOperator",
        "description": {
          "fields": "[gender, avg(age)]"
        },
        "children": [
          {
            "name": "OpenSearchIndexScan",
            "description": {
              "request": "OpenSearchQueryRequest(indexName=accounts, sourceBuilder={\"from\":0,\"size\":0,\"timeout\":\"1m\",\"aggregations\":{\"composite_buckets\":{\"composite\":{\"size\":1000,\"sources\":[{\"gender\":{\"terms\":{\"field\":\"gender.keyword\",\"missing_bucket\":true,\"missing_order\":\"first\",\"order\":\"asc\"}}}]},\"aggregations\":{\"avg(age)\":{\"avg\":{\"field\":\"age\"}}}}}}, searchDone=false)"
            },
            "children": []
          }
        ]
      }
    }

Sort Merge Into OpenSearch Aggregation
--------------------------------------

The Sort operator will merge into OpenSearch Aggregation.::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_sql/_explain \
    ... -d '{"query" : "SELECT gender, avg(age) FROM accounts GROUP BY gender ORDER BY gender DESC NULLS LAST"}'
    {
      "root": {
        "name": "ProjectOperator",
        "description": {
          "fields": "[gender, avg(age)]"
        },
        "children": [
          {
            "name": "OpenSearchIndexScan",
            "description": {
              "request": "OpenSearchQueryRequest(indexName=accounts, sourceBuilder={\"from\":0,\"size\":0,\"timeout\":\"1m\",\"aggregations\":{\"composite_buckets\":{\"composite\":{\"size\":1000,\"sources\":[{\"gender\":{\"terms\":{\"field\":\"gender.keyword\",\"missing_bucket\":true,\"missing_order\":\"last\",\"order\":\"desc\"}}}]},\"aggregations\":{\"avg(age)\":{\"avg\":{\"field\":\"age\"}}}}}}, searchDone=false)"
            },
            "children": []
          }
        ]
      }
    }


Because the OpenSearch Composite Aggregation doesn't support order by metrics field, then if the sort list include fields which refer to metrics aggregation, then the sort operator can't be push down to OpenSearch Aggregation::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST localhost:9200/_plugins/_sql/_explain \
    ... -d '{"query" : "SELECT gender, avg(age) FROM accounts GROUP BY gender ORDER BY avg(age)"}'
    {
      "root": {
        "name": "ProjectOperator",
        "description": {
          "fields": "[gender, avg(age)]"
        },
        "children": [
          {
            "name": "SortOperator",
            "description": {
              "sortList": {
                "avg(age)": {
                  "sortOrder": "ASC",
                  "nullOrder": "NULL_FIRST"
                }
              }
            },
            "children": [
              {
                "name": "OpenSearchIndexScan",
                "description": {
                  "request": "OpenSearchQueryRequest(indexName=accounts, sourceBuilder={\"from\":0,\"size\":0,\"timeout\":\"1m\",\"aggregations\":{\"composite_buckets\":{\"composite\":{\"size\":1000,\"sources\":[{\"gender\":{\"terms\":{\"field\":\"gender.keyword\",\"missing_bucket\":true,\"missing_order\":\"first\",\"order\":\"asc\"}}}]},\"aggregations\":{\"avg(age)\":{\"avg\":{\"field\":\"age\"}}}}}}, searchDone=false)"
                },
                "children": []
              }
            ]
          }
        ]
      }
    }

Limitations on Query Optimizations
==================================

Multi-fields in WHERE Conditions
--------------------------------

The filter expressions in ``WHERE`` clause may be pushed down to OpenSearch DSL queries to avoid large amounts of data retrieved. In this case, for OpenSearch multi-field (a text field with another keyword field inside), assumption is made that the keyword field name is always "keyword" which is true by default.

Multiple Window Functions
-------------------------

At the moment there is no optimization to merge similar sort operators to avoid unnecessary sort. In this case, only one sort operator associated with window function will be pushed down to OpenSearch DSL queries. Others will sort the intermediate results in memory and return to its window operator in the upstream. This cost can be avoided by optimization aforementioned though in-memory sorting operation can still happen. Therefore a custom circuit breaker is in use to monitor sort operator and protect memory usage.

Sort Push Down
--------------
Without sort push down optimization, the sort operator will sort the result from child operator. By default, only 10000 docs will extracted from the source index, `you can change this value by using size_limit setting <../admin/settings.rst#opensearch-query-size-limit>`_.
