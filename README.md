Hi :wave:, 

I've written my answers to this technical test as part of a dbt project. If the question asked for a query it is saved within `question-queries`, and if the question asked for a table to be created, it is saved within `models`. I've also included the full queries for question 3 & 4/5, for reference. 

As I already have a dbt project set up on my laptop, and want to avoid overwriting anything, I haven't set up the dbt project fully. Normally I would check the models I've included the profiles yml I would use, and the table structure I would introduce to create the tables, but also follow DRY principles.

For question 3 I split the table up into separate components, because the query was becoming very long, and there were elements that could be useful elswhere, such as having an overall film ranking table. 

For question 4, the same approach could be taken, but I've kept all the sql to one query to demonstrate the difference between incremental models and full refreshes. 

With more time, I would create a yml file for each model to document what each column does, and also add relevant tests to ensure data accuracy. I have completed one yml file, for `film_recommendations` as an example.

Hope everything makes sense!