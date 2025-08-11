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

// Person -> Phone
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_uses_phone.csv' AS row
WITH row WHERE row.person_id IS NOT NULL AND row.person_id <> '' AND row.number IS NOT NULL AND row.number <> ''
MATCH (p:Person {person_id: row.person_id})
MERGE (ph:Phone {number: row.number})
MERGE (p)-[:USES_PHONE]->(ph);

// Person -> IP
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_logged_from.csv' AS row
WITH row WHERE row.person_id IS NOT NULL AND row.person_id <> '' AND row.value IS NOT NULL AND row.value <> ''
MATCH (p:Person {person_id: row.person_id})
MERGE (ip:IP {value: row.value})
MERGE (p)-[:LOGGED_FROM]->(ip);

// Person -> Company
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/gabmansur/fraud-graph-data/master/data/processed/relationships_works_for.csv' AS row
WITH row WHERE row.person_id IS NOT NULL AND row.person_id <> '' AND row.company IS NOT NULL AND row.company <> ''
MATCH (p:Person {person_id: row.person_id})
MERGE (c:Company {name: row.company})
MERGE (p)-[:WORKS_FOR]->(c);