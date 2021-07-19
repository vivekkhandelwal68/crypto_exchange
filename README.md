# crypto_exchange
This project takes help of AWS lambda, AWS CloudWatch and Snowflake Datawarehouse to make realtime crypto currency cnonversion.

1) AWS Cloud watch has been scheduled to generate an event per minute.
2) These events hves been targeted to AWS Lamda function(crypto_exchange). These event trigger this lambda function.
3) For our lambda function to work, we need requests and Snowflake connector to python modules.
4) We have downloaded and uploaded these modules in a lambda layer. We have added this layer with our lambda function.
5) now our lambda function can access these modules.
6) We make REST API call with the help of requests module and get response.
7) we extract required info from response and write reponse as a file into s3.
8) We connect with Snowflake and truncate the Stage table.
9) once table is truncated, lambda execute copy command and load stage table from file present in S3 location.
10) Once data is loaded in Stage table(EXCHANGE_STG). Stream(exchange_stream) associated with this table can detect these cdc changes.
11) We have created a task(EXCHANGE_LOAD_TASK) which is scheduled every 2 min, This task flatten response from Stage table, join it with Dimension table(COINS_MAIN_DIM) and load the result in fact table(exchange_fact).
12) We have created a view(AVG_COIN_PRICE_BY_DAYNO) on top of fact table. This view gives daly avg. value of each crypto currency. Now we can use this view in our dashboard to show comaprison.
