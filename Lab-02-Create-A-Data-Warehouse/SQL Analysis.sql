/* 
1. How many animals of each type have outcomes? 
I.e. how many cats, dogs, birds etc. Note that this question is asking about number of animals, not number of outcomes, so animals with multiple outcomes should be counted only once.
*/
SELECT "Animal Type", COUNT(DISTINCT "Animal Dimension"."Animal Key") AS "Number of Animals"
FROM "Animal Dimension"
INNER JOIN "Fact Animal Outcome" ON "Animal Dimension"."Animal Key" = "Fact Animal Outcome"."Animal Key"
GROUP BY "Animal Type";
-----------------------------------------------------------------------------------------------------------------------

/* 
2. How many animals are there with more than 1 outcome?
*/
SELECT COUNT("Animal Key") AS "Number of Animals"
FROM (
    SELECT "Animal Key"
    FROM "Fact Animal Outcome"
    GROUP BY "Animal Key"
    HAVING COUNT(*) > 1
) AS "AnimalsWithMultipleOutcomes";
-----------------------------------------------------------------------------------------------------------------------

/* 
3. What are the top 5 months for outcomes? 
Calendar months in general, not months of a particular year. This means answer will be like April, October, etc rather than April 2013, October 2018, 
*/

SELECT TO_CHAR("DateTime"::DATE, 'Month') AS "Month", COUNT(*) AS "Number of Outcomes"
FROM "Date Dimension"
INNER JOIN "Fact Animal Outcome" ON "Date Dimension"."Date Key" = "Fact Animal Outcome"."Date Key"
GROUP BY "Month"
ORDER BY "Number of Outcomes" DESC
LIMIT 5;
-----------------------------------------------------------------------------------------------------------------------

/* 
A "Kitten" is a "Cat" who is less than 1 year old. A "Senior cat" is a "Cat" who is over 10 years old. An "Adult" is a cat who is between 1 and 10 years old.

    What is the total number percentage of kittens, adults, and seniors, whose outcome is "Adopted"?
    Conversely, among all the cats who were "Adopted", what is the total number percentage of kittens, adults, and seniors?
*/
WITH AdoptedCats AS (
    SELECT
        "Animal Dimension"."Animal Key",
        "Animal Type",
        DATE_PART('year', AGE("Date of Birth")) AS "Age"
    FROM "Animal Dimension"
    LEFT JOIN "Fact Animal Outcome" ON "Animal Dimension"."Animal Key" = "Fact Animal Outcome"."Animal Key"
    LEFT JOIN "Outcome Type Dimension" ON "Outcome Type Dimension"."Outcome Type Key" = "Fact Animal Outcome"."Outcome Type Key"
    WHERE "Animal Type" = 'Cat' AND "Outcome Type" = 'Adoption'
)
SELECT
    "Age" AS "Cat Age",
    COUNT("Animal Key") AS "Number of Cats",
    (COUNT("Animal Key") * 100.0 / SUM(COUNT("Animal Key")) OVER ()) AS "Percentage"
FROM AdoptedCats
GROUP BY "Age";

-----------------------------------------------------------------------------------------------------------------------

/* 
5. For each date, what is the cumulative total of outcomes up to and including this date?
*/
SELECT
    Date("DateTime"),
    SUM(COUNT(Date("DateTime"))) OVER (ORDER BY Date("DateTime")) AS "Cumulative Total Outcomes"
    FROM "Fact Animal Outcome"  
    LEFT JOIN "Date Dimension" ON "Fact Animal Outcome"."Date Key" = "Date Dimension"."Date Key"  
    GROUP BY Date("DateTime");

