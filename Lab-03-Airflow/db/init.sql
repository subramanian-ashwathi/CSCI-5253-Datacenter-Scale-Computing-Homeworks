-- Create Animal Dimension Table
CREATE TABLE IF NOT EXISTS "Animal Dimension" (
    "Animal Key" SERIAL PRIMARY KEY,
    "Animal ID" VARCHAR(255),
    "Name" VARCHAR(255),
    "Date of Birth" DATE,
    "Animal Type" VARCHAR(255),
    "Property" VARCHAR(255),
    "Sex" VARCHAR(255),
    "Breed" VARCHAR(255),
    "Color" VARCHAR(255)
);

-- Create Outcome Type Dimension Table
CREATE TABLE IF NOT EXISTS "Outcome Type Dimension" (
    "Outcome Type Key" SERIAL PRIMARY KEY,
    "Outcome Type" VARCHAR(255)
);

-- Create Outcome Subtype Dimension Table
CREATE TABLE IF NOT EXISTS "Outcome Subtype Dimension" (
    "Outcome Subtype Key" SERIAL PRIMARY KEY,
    "Outcome Subtype" VARCHAR(255)
);

-- Create Age Dimension Table
CREATE TABLE IF NOT EXISTS "Age Dimension" (
    "Age Key" SERIAL PRIMARY KEY,
    "Age upon Outcome" VARCHAR(255)
);

-- Create Date Dimension Table
CREATE TABLE IF NOT EXISTS "Date Dimension" (
    "Date Key" SERIAL PRIMARY KEY,
    "DateTime" TIMESTAMP
);

-- Create Fact Animal Outcome Table
CREATE TABLE IF NOT EXISTS "Fact Animal Outcome" (
    "Animal Key" INT,
    "Outcome Type Key" INT,
    "Outcome Subtype Key" INT,
    "Date Key" INT,
    "Age Key" INT
);

-- Add foreign key constraints for dimension tables
ALTER TABLE "Fact Animal Outcome"
ADD CONSTRAINT "FK Animal" FOREIGN KEY ("Animal Key") REFERENCES "Animal Dimension"("Animal Key");

ALTER TABLE "Fact Animal Outcome"
ADD CONSTRAINT "FK Outcome" FOREIGN KEY ("Outcome Type Key") REFERENCES "Outcome Type Dimension"("Outcome Type Key");

ALTER TABLE "Fact Animal Outcome"
ADD CONSTRAINT "FK Outcome Subtype" FOREIGN KEY ("Outcome Subtype Key") REFERENCES "Outcome Subtype Dimension"("Outcome Subtype Key");

ALTER TABLE "Fact Animal Outcome"
ADD CONSTRAINT "FK Date" FOREIGN KEY ("Date Key") REFERENCES "Date Dimension"("Date Key");

ALTER TABLE "Fact Animal Outcome"
ADD CONSTRAINT "FK Age" FOREIGN KEY ("Age Key") REFERENCES "Age Dimension"("Age Key");