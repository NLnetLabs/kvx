DROP TABLE IF EXISTS store;
CREATE TABLE store (
  "scope" TEXT[] NOT NULL,
  "key" VARCHAR NOT NULL,
  "value" JSONB NOT NULL,
  PRIMARY KEY("scope", "key")
);
