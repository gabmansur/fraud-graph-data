// Load Person nodes
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/nodes_person.csv' AS row
MERGE (p:Person {user_id: row.user_id})
SET p.name = row.name,
    p.email = row.email;

// Load Phone nodes
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/nodes_phone.csv' AS row
MERGE (:Phone {number: row.phone});

// Load IP nodes
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/nodes_ip.csv' AS row
MERGE (:IP {address: row.ip});

// Load Company nodes
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/nodes_company.csv' AS row
MERGE (:Company {name: row.company});

// SENT transactions
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_sent.csv' AS row
MATCH (a:Person {user_id: row.from})
MATCH (b:Person {user_id: row.to})
MERGE (a)-[r:SENT]->(b)
SET r.amount = toFloat(row.amount),
    r.type = row.type,
    r.isFraud = (row.isFraud = '1');

// USES_PHONE
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_uses_phone.csv' AS row
MATCH (p:Person {user_id: row.user_id})
MATCH (ph:Phone {number: row.phone})
MERGE (p)-[:USES_PHONE]->(ph);

// LOGGED_FROM
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_logged_from.csv' AS row
MATCH (p:Person {user_id: row.user_id})
MATCH (ip:IP {address: row.ip})
MERGE (p)-[:LOGGED_FROM]->(ip);

// WORKS_FOR
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_works_for.csv' AS row
MATCH (p:Person {user_id: row.user_id})
MATCH (c:Company {name: row.company})
MERGE (p)-[:WORKS_FOR]->(c);