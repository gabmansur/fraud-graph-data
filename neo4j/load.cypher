// ---------- Constraints (safe to re-run) ----------
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person)  REQUIRE p.person_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (ph:Phone)  REQUIRE ph.number    IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (ip:IP)     REQUIRE ip.value     IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Company) REQUIRE c.name       IS UNIQUE;

// Optional marker so you can confirm last load time
WITH datetime() AS ts
MERGE (m:Meta {id:'load'}) SET m.last_load = ts;

// ---------- NODES ----------
// Persons
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/nodes_person.csv' AS row
WITH row WHERE row.person_id IS NOT NULL AND row.person_id <> ''
MERGE (p:Person {person_id: row.person_id})
SET p.name = row.name, p.email = row.email;

// Phones
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/nodes_phone.csv' AS row
WITH row WHERE row.number IS NOT NULL AND row.number <> ''
MERGE (:Phone {number: row.number});

// IPs
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/nodes_ip.csv' AS row
WITH row WHERE row.value IS NOT NULL AND row.value <> ''
MERGE (:IP {value: row.value});

// Companies
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/nodes_company.csv' AS row
WITH row WHERE row.name IS NOT NULL AND row.name <> ''
MERGE (:Company {name: row.name});

// ---------- RELATIONSHIPS ----------
// SENT (transactions)
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_sent.csv' AS row
WITH row WHERE row.from IS NOT NULL AND row.from <> '' AND row.to IS NOT NULL AND row.to <> ''
MATCH (a:Person {person_id: row.from})
MATCH (b:Person {person_id: row.to})
MERGE (a)-[r:SENT]->(b)
SET r.amount  = toFloat(row.amount),
    r.type    = row.type,
    r.isFraud = (row.isFraud = '1');

// Person -> Phone
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_uses_phone.csv' AS row
WITH row WHERE row.person_id IS NOT NULL AND row.person_id <> '' AND row.number IS NOT NULL AND row.number <> ''
MATCH (p:Person {person_id: row.person_id})
MERGE (ph:Phone {number: row.number})
MERGE (p)-[:USES_PHONE]->(ph);

// Person -> IP
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_logged_from.csv' AS row
WITH row WHERE row.person_id IS NOT NULL AND ro_