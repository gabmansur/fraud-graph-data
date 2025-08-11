// ---------- Constraints (safe to re-run) ----------
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person)  REQUIRE p.person_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (ph:Phone)  REQUIRE ph.number    IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (ip:IP)     REQUIRE ip.value     IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Company) REQUIRE c.name       IS UNIQUE;

// ---------- Nodes ----------
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/nodes_person.csv' AS row
MERGE (p:Person {person_id: row.person_id})
SET p.name = row.name, p.email = row.email;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/nodes_phone.csv' AS row
MERGE (:Phone {number: row.number});

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/nodes_ip.csv' AS row
MERGE (:IP {value: row.value});

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/nodes_company.csv' AS row
MERGE (:Company {name: row.name});

// ---------- Transactions ----------
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_sent.csv' AS row
MATCH (a:Person {person_id: row.from})
MATCH (b:Person {person_id: row.to})
MERGE (a)-[r:SENT]->(b)
SET r.amount  = toFloat(row.amount),
    r.type    = row.type,
    r.isFraud = (row.isFraud = '1');

// ---------- Person -> Phone ----------
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_uses_phone.csv' AS row
MATCH (p:Person {person_id: row.person_id})
MERGE (ph:Phone {number: row.number})
MERGE (p)-[:USES_PHONE]->(ph);

// ---------- Person -> IP ----------
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_logged_from.csv' AS row
MATCH (p:Person {person_id: row.person_id})
MERGE (ip:IP {value: row.value})
MERGE (p)-[:LOGGED_FROM]->(ip);

// ---------- Person -> Company ----------
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_works_for.csv' AS row
MATCH (p:Person {person_id: row.person_id})
MERGE (c:Company {name: row.company})
MERGE (p)-[:WORKS_FOR]->(c);