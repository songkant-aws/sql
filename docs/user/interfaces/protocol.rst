.. highlight:: sh

========
Protocol
========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1


Introduction
============

For the protocol, SQL plugin provides multiple response formats for different purposes while the request format is same for all. Among them JDBC format is widely used because it provides schema information and more functionality such as pagination. Besides JDBC driver, various clients can benefit from the detailed and well formatted response.


Request Format
==============

Description
-----------

The body of HTTP POST request can take a few more other fields with SQL query.

Example 1
---------

Use `filter` to add more conditions to OpenSearch DSL directly.

SQL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql -d '{
	  "query" : "SELECT firstname, lastname, balance FROM accounts",
	  "filter" : {
	    "range" : {
	      "balance" : {
	        "lt" : 10000
	      }
	    }
	  }
	}'

Explain::

	{
	  "from" : 0,
	  "size" : 200,
	  "query" : {
	    "bool" : {
	      "filter" : [
	        {
	          "bool" : {
	            "filter" : [
	              {
	                "range" : {
	                  "balance" : {
	                    "from" : null,
	                    "to" : 10000,
	                    "include_lower" : true,
	                    "include_upper" : false,
	                    "boost" : 1.0
	                  }
	                }
	              }
	            ],
	            "adjust_pure_negative" : true,
	            "boost" : 1.0
	          }
	        }
	      ],
	      "adjust_pure_negative" : true,
	      "boost" : 1.0
	    }
	  },
	  "_source" : {
	    "includes" : [
	      "firstname",
	      "lastname",
	      "balance"
	    ],
	    "excludes" : [ ]
	  }
	}

Example 2
---------

Use `parameters` for actual parameter value in prepared SQL query.

SQL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql -d '{
	  "query" : "SELECT * FROM accounts WHERE age = ?",
	  "parameters" : [
	    {
	      "type" : "integer",
	      "value" : 30
	    }
	  ]
	}'

Explain::

	{
	  "from" : 0,
	  "size" : 200,
	  "query" : {
	    "bool" : {
	      "filter" : [
	        {
	          "bool" : {
	            "must" : [
	              {
	                "term" : {
	                  "age" : {
	                    "value" : 30,
	                    "boost" : 1.0
	                  }
	                }
	              }
	            ],
	            "adjust_pure_negative" : true,
	            "boost" : 1.0
	          }
	        }
	      ],
	      "adjust_pure_negative" : true,
	      "boost" : 1.0
	    }
	  }
	}

JDBC Format
===========

Description
-----------

By default the plugin return JDBC format. JDBC format is provided for JDBC driver and client side that needs both schema and result set well formatted.

Example 1
---------

Here is an example for normal response. The `schema` includes field name and its type and `datarows` includes the result set.

SQL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql -d '{
	  "query" : "SELECT firstname, lastname, age FROM accounts ORDER BY age LIMIT 2"
	}'

Result set::

	{
	  "schema" : [
	    {
	      "name" : "firstname",
	      "type" : "text"
	    },
	    {
	      "name" : "lastname",
	      "type" : "text"
	    },
	    {
	      "name" : "age",
	      "type" : "long"
	    }
	  ],
	  "total" : 4,
	  "datarows" : [
	    [
	      "Nanette",
	      "Bates",
	      28
	    ],
	    [
	      "Amber",
	      "Duke",
	      32
	    ]
	  ],
	  "size" : 2,
	  "status" : 200
	}

Example 2
---------

If any error occurred, error message and the cause will be returned instead.

SQL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql?format=jdbc -d '{
	  "query" : "SELECT unknown FROM accounts"
	}'

Result set::

	{
	  "error" : {
	    "reason" : "Invalid SQL query",
	    "details" : "Field [unknown] cannot be found or used here.",
	    "type" : "SemanticAnalysisException"
	  },
	  "status" : 400
	}

CSV Format
==========

Description
-----------

You can also use CSV format to download result set as CSV

Example
-------

SQL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql?format=csv -d '{
	  "query" : "SELECT firstname, lastname, age FROM accounts ORDER BY age"
	}'

Result set::

	firstname,lastname,age
	Nanette,Bates,28
	Amber,Duke,32
	Dale,Adams,33
	Hattie,Bond,36


The formatter sanitizes the csv result with the following rules:

1. If a header cell or data cell is starting with special character including '+', '-', '=' , '@', the sanitizer will insert a single-quote at the start of the cell.

2. If there exists one or more commas (','), the sanitizer will quote the cell with double quotes.

For example::

    >> curl -H 'Content-Type: application/json' -X PUT localhost:9200/userdata/_doc/1?refresh=true -d '{
      "+firstname": "-Hattie",
      "=lastname": "@Bond",
      "address": "671 Bristol Street, Dente, TN"
    }'
	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql?format=csv -d '{
	  "query" : "SELECT firstname, lastname, address FROM userdata"
	}'

Result set::

    '+firstname,'=lastname,address
    'Hattie,'@Bond,"671 Bristol Street, Dente, TN"


If you prefer escaping the sanitization and keeping the original csv result, you can add a "sanitize" param and set it to false value to skip sanitizing. For example::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql?format=csv&sanitize=false -d '{
	  "query" : "SELECT firstname, lastname, address FROM userdata"
	}'

Result set::

    +firstname,=lastname,address
    Hattie,@Bond,671 Bristol Street, Dente, TN
	

RAW Format
==========

Description
-----------

Additionally raw format can be used to pipe the result to other command line tool for post processing, fields are delimited by pipe
character '|' vs common charactoer used in CSV format

Example
-------

SQL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql?format=raw -d '{
	  "query" : "SELECT firstname, lastname, age FROM accounts ORDER BY age"
	}'

Result set::

	firstname|lastname|age
	Nanette|Bates|28
	Amber|Duke|32
	Dale|Adams|33
	Hattie|Bond|36


The formatter sanitizes the raw result with the following rules:

1. If there exists one or more pipes ('|'), the sanitizer will quote the cell with double quotes.

For example::

    >> curl -H 'Content-Type: application/json' -X PUT localhost:9200/userdata/_doc/1?refresh=true -d '{
      "+firstname": "-Hattie",
      "=lastname": "@Bond",
      "address": "671 Bristol Street|, Dente, TN"
    }'
	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql?format=csv -d '{
	  "query" : "SELECT firstname, lastname, address FROM userdata"
	}'

Result set::

    '+firstname|'=lastname|address
    'Hattie|@Bond|"671 Bristol Street|, Dente, TN"


Visualization Format
====================

To support the Observability visualizations we also provide a new protocol that formats the data in columns for PPL. You can specify the format as "viz" to apply this format to your response, the response is formatted as compact json by default, for example::

    >> curl -H 'Content-Type: application/json -X POST localhost:9200/_plugins/_ppl?format=viz' -d '{
      "query": "source=accounts"
    }'

Result set::

    {"data":{"account_number":[1,6,13,18],"firstname":["Amber","Hattie","Nanette","Dale"],"address":["880 Holmes Lane","671 Bristol Street","789 Madison Street","467 Hutchinson Court"],"balance":[39225,5686,32838,4180],"gender":["M","M","F","M"],"city":["Brogan","Dante","Nogal","Orick"],"employer":["Pyrami","Netagy","Quility",null],"state":["IL","TN","VA","MD"],"age":[32,36,28,33],"email":["amberduke@pyrami.com","hattiebond@netagy.com","nanettebates@quility.com","daleadams@boink.com"],"lastname":["Duke","Bond","Bates","Adams"]},"fields":[{"name":"account_number","type":"long"},{"name":"firstname","type":"text"},{"name":"address","type":"text"},{"name":"balance","type":"long"},{"name":"gender","type":"text"},{"name":"city","type":"text"},{"name":"employer","type":"text"},{"name":"state","type":"text"},{"name":"age","type":"long"},{"name":"email","type":"text"},{"name":"lastname","type":"text"}],"size":4,"status":200}


You can also shape the format to pretty json by adding additional param ``pretty`` set it to true ``pretty=true``, for example::

    >> curl -H 'Content-Type: application/json -X POST localhost:9200/_plugins/_ppl?format=viz&pretty' -d '{
      "query": "source=accounts"
    }'

Result set::

    {
      "data": {
        "account_number": [
          1,
          6,
          13,
          18
        ],
        "firstname": [
          "Amber",
          "Hattie",
          "Nanette",
          "Dale"
        ],
        "address": [
          "880 Holmes Lane",
          "671 Bristol Street",
          "789 Madison Street",
          "467 Hutchinson Court"
        ],
        "balance": [
          39225,
          5686,
          32838,
          4180
        ],
        "gender": [
          "M",
          "M",
          "F",
          "M"
        ],
        "city": [
          "Brogan",
          "Dante",
          "Nogal",
          "Orick"
        ],
        "employer": [
          "Pyrami",
          "Netagy",
          "Quility",
          null
        ],
        "state": [
          "IL",
          "TN",
          "VA",
          "MD"
        ],
        "age": [
          32,
          36,
          28,
          33
        ],
        "email": [
          "amberduke@pyrami.com",
          "hattiebond@netagy.com",
          "nanettebates@quility.com",
          "daleadams@boink.com"
        ],
        "lastname": [
          "Duke",
          "Bond",
          "Bates",
          "Adams"
        ]
      },
      "fields": [
        {
          "name": "account_number",
          "type": "long"
        },
        {
          "name": "firstname",
          "type": "text"
        },
        {
          "name": "address",
          "type": "text"
        },
        {
          "name": "balance",
          "type": "long"
        },
        {
          "name": "gender",
          "type": "text"
        },
        {
          "name": "city",
          "type": "text"
        },
        {
          "name": "employer",
          "type": "text"
        },
        {
          "name": "state",
          "type": "text"
        },
        {
          "name": "age",
          "type": "long"
        },
        {
          "name": "email",
          "type": "text"
        },
        {
          "name": "lastname",
          "type": "text"
        }
      ],
      "size": 4,
      "status": 200
    }

