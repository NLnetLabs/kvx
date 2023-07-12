DROP TABLE IF EXISTS store;
CREATE TABLE store (
  "namespace" VARCHAR NOT NULL,
  "scope" TEXT[] NOT NULL,
  "key" VARCHAR NOT NULL,
  "value" JSONB NOT NULL,
  PRIMARY KEY("namespace", "scope", "key")
);
