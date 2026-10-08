# Databricks notebook source
#our goal is to dynamically apply join between fact table and dimuser and dimartist
#We will start wih creating array called parameters
#dictionary indside it

# COMMAND ----------

parameters=[

#for table factstream, it is the base table , follow order for applying join
#first factstream, then dimuser, then dimartist
#obviously there will not be joined condition for factstream
#so we will have in factstream table
#catalog name to be more specific or dynamic
    {
        'table':'spotify_catalog.silver.factstream',
        'alias':'factstream',
        'cols':'factstream.stream_id, factstream.listen_duration'
       # 'where':'' if you have condition
    },

#for table dimuser
#catalog name to be more specific or dynamic
    {
        'table':'spotify_catalog.silver.dimuser',
        'alias':'dimuser',
        'cols':'dimuser.user_id, dimuser.user_name',
       # 'where':'' if you have condition
         'condition':'factstream.user_id = dimuser.user_id'
    },

    #for table dimtrack
    #catalog name to be more specific or dynamic
    {
        'table':'spotify_catalog.silver.dimtrack',
        'alias':'dimtrack',
        'cols':'dimtrack.track_id, dimtrack.track_name',
       # 'where':'' if you have condition
       'condition':'factstream.track_id = dimtrack.track_id'

    }
]

# COMMAND ----------

#Now lets use JINJA 
#Install jinja
#%pip install jinja2
%pip install jinja2



# COMMAND ----------

#import template
from jinja2 import Template

# COMMAND ----------

#what will happen?
#First of all it prepare our text. called as query_text
#then i will be using all column
#how i can use it?
#i will be using it from above created array.
#we wanna load column from that array one by one
#for that need to run a loop.
#once select and from query is done jump to join 
#finally parameterized query is ready
#test it just run the cell as it is just a string


query_text= """
            SELECT
                {% for param in parameters %}
                    {{ param.cols }}
                        {% if not loop.last %}
                            ,
                        {% endif %}
                {% endfor %}
            FROM
                {% for param in parameters %}
                    {% if loop.first %}
                        {{ param['table'] }} AS {{ param['alias'] }}
                    {% endif %}
                {% endfor %}
                {% for param in parameters %}
                    {% if not loop.first %}
                    LEFT JOIN
                        {{ param['table'] }} AS {{ param['alias'] }}
                    ON
                        {{ param['condition'] }}
                    {% endif %}
                {% endfor %}    
"""

# COMMAND ----------

#make this string as jinja string using template

jinja_sql_str = Template(query_text)
query = jinja_sql_str.render(parameters=parameters)
print(query)

# COMMAND ----------

#the run code is handling all the join, from clause, on condition
#it is also handling all the columns as well
#we can also define any transformation within this paramter
#In real world what will happen is that you will get business requirements
#like perform join on this table, show this column and on this condition
#To achieve it populate the paramter key value pair and run the code
#lets try below

# COMMAND ----------

spark.sql(query)

# COMMAND ----------

display(spark.sql(query))