--SELECT * FROM "powers";
DROP TABLE powers;
SELECT * FROM "powers";
--
--CREATE VIEW "powers" (pk VARCHAR PRIMARY KEY, "personal"."hero" VARCHAR, "personal"."power" VARCHAR,
--"professional"."name" VARCHAR, "professional"."xp" VARCHAR, "custom"."color" VARCHAR);

--SELECT p.name as Name1, p1.name as Name2, p.power as Power
--FROM "powers" as p
--INNER JOIN "powers" as p1
--    ON p."power" = p1."power"
--    AND p."hero" = "yes" and p1."hero" = "yes";
