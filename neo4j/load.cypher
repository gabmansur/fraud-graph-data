// Load Person nodes
LOAD CSV WITH HEADERS FROM 'file:///nodes_person.csv' AS row
MERGE (p:Person {user_id: row.user_id})
SET p.name = row.name,
    p.email = row.email;

// Load Phone nodes
LOAD CSV WITH HEADERS FROM 'file:///nodes_phone.csv' AS row
MERGE (:Phone {number: row.phone});

// Load IP nodes
LOAD CSV WITH HEADERS FROM 'file:///nodes_ip.csv' AS row
MERGE (:IP {address: row.ip});

// Load Company nodes
LOAD CSV WITH HEADERS FROM 'file:///nodes_company.csv' AS row
MERGE (:Company {name: row.company});

// Load SENT transactions
LOAD CSV WITH HEADERS FROM 'file:///relationships_sent.csv' AS row
MATCH (a:Person {user_id: row.from})
MATCH (b:Person {user_id: row.to})
MERGE (a)-[r:SENT]->(b)
SET r.amount = toFloat(row.amount),
    r.type = row.type,
    r.isFraud = row.isFraud = '1';

// Load USES_PHONE relationships
LOAD CSV WITH HEADERS FROM 'file:///relationships_uses_phone.csv' AS row
MATCH (p:Person {user_id: row.user_id})
MATCH (ph:Phone {number: row.phone})
MERGE (p)-[:USES_PHONE]->(ph);

// Load LOGGED_FROM relationships
LOAD CSV WITH HEADERS FROM 'file:///relationships_logged_from.csv' AS row
MATCH (p:Person {user_id: row.user_id})
MATCH (ip:IP {address: row.ip})
MERGE (p)-[:LOGGED_FROM]->(ip);

//  Load WORKS_FOR relationships
LOAD CSV WITH HEADERS FROM 'file:///relationships_works_for.csv' AS row
MATCH (p:Person {user_id: row.user_id})
MATCH (c:Company {name: row.company})
MERGE (p)-[:WORKS_FOR]->(c);
