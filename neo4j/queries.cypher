/* ===========================================================
   FRAUD GRAPH – QUERY CATALOG
   -----------------------------------------------------------
   A. Schema & Indexes
     A1. Constraints (unique keys per entity)
     A2. Performance Indexes

   B. Sanity & Exploration
     B1. Counts by label
     B2. Relationship counts by type
     B3. Example: top-degree People (hubs)

   C. Phase Queries
     C1. Phase 1: Basic network (neighbors, ego nets)
     C2. Phase 2: Shared Phone overlaps
     C3. Phase 3: Shared IP overlaps
     C4. Phone + IP conjunct overlap (stronger signal)

   D. Fraud Patterns & Scores (starter set)
     D1. Short money loops (2–5 hops)
     D2. Fan-in/Fan-out anomalies
     D3. Multi-identifier linkage (phone, ip, device, email)
     D4. Simple risk score (rule-based)

   E. Maintenance / Utilities
     E1. Sample upsert guards
     E2. De-dup helpers (cautious)
   =========================================================== */


/* ===========================
   A. SCHEMA & INDEXES
   =========================== */

// A1. Constraints (idempotent)
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.person_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (ph:Phone)  REQUIRE ph.number   IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (ip:IP)     REQUIRE ip.value    IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (d:Device)  REQUIRE d.device_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (a:Account) REQUIRE a.account_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (tx:Txn)    REQUIRE tx.txn_id    IS UNIQUE;

// A2. Helpful indexes
CREATE INDEX IF NOT EXISTS FOR (p:Person) ON (p.name);
CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.iban);
CREATE INDEX IF NOT EXISTS FOR (tx:Txn) ON (tx.timestamp);
CREATE INDEX IF NOT EXISTS FOR (ip:IP) ON (ip.last_seen);


/* ===========================
   B. SANITY & EXPLORATION
   =========================== */

// B1. Counts by label
CALL db.labels() YIELD label
CALL {
  WITH label
  RETURN label AS l, count(*) AS n
  FROM (MATCH (n:`${label}`) RETURN n)
}
RETURN l AS label, n
ORDER BY n DESC;

// B2. Relationship counts by type
CALL db.relationshipTypes() YIELD relationshipType AS type
CALL {
  WITH type
  MATCH ()-[r:`${type}`]->()
  RETURN count(r) AS n
}
RETURN type, n
ORDER BY n DESC;

// B3. Top-degree People (hubs)
MATCH (p:Person)
RETURN p.person_id AS person, COUNT { (p)--() } AS deg
ORDER BY deg DESC
LIMIT 20;


/* ===========================
   C. PHASE QUERIES
   =========================== */

// C1. Phase 1 – local neighborhood / ego-net (2 hops)
MATCH (p:Person {person_id: "P-0001"})-[r*1..2]-(x)
RETURN p, r, x;

// C2. Phase 2 – shared Phone overlaps (unique pairs)
MATCH (p1:Person)-[:USES_PHONE]->(ph:Phone)<-[:USES_PHONE]-(p2:Person)
WHERE id(p1) < id(p2)
RETURN ph.number AS phone, collect(p1.person_id) AS group1, collect(p2.person_id) AS group2, size(collect(p1))+size(collect(p2)) AS involved
ORDER BY involved DESC
LIMIT 50;

// Also as edge list of Person–Person overlap via Phone
MATCH (p1:Person)-[:USES_PHONE]->(ph:Phone)<-[:USES_PHONE]-(p2:Person)
WHERE id(p1) < id(p2)
RETURN p1.person_id AS src, p2.person_id AS dst, "PHONE" AS reason, ph.number AS key
ORDER BY src, dst;

// C3. Phase 3 – shared IP overlaps (unique pairs)
MATCH (p1:Person)-[:LOGGED_FROM]->(ip:IP)<-[:LOGGED_FROM]-(p2:Person)
WHERE id(p1) < id(p2)
RETURN ip.value AS ip, collect(p1.person_id) AS group1, collect(p2.person_id) AS group2, size(collect(p1))+size(collect(p2)) AS involved
ORDER BY involved DESC
LIMIT 50;

// Edge list for IP overlap
MATCH (p1:Person)-[:LOGGED_FROM]->(ip:IP)<-[:LOGGED_FROM]-(p2:Person)
WHERE id(p1) < id(p2)
RETURN p1.person_id AS src, p2.person_id AS dst, "IP" AS reason, ip.value AS key
ORDER BY src, dst;

// C4. Stronger signal – shared Phone AND shared IP
MATCH (p1:Person)-[:USES_PHONE]->(ph:Phone)<-[:USES_PHONE]-(p2:Person),
      (p1)-[:LOGGED_FROM]->(ip:IP)<-[:LOGGED_FROM]-(p2:Person)
WHERE id(p1) < id(p2)
RETURN p1.person_id AS p_left, p2.person_id AS p_right, ph.number AS phone, ip.value AS ip
ORDER BY p_left, p_right
LIMIT 100;


/* ===========================
   D. FRAUD PATTERNS & SCORES
   =========================== */

// D1. Short loops (2..5 hops) returning to same account
MATCH p = (a:Account)-[:SENT*2..5]->(a)
WHERE ALL(rel IN relationships(p) WHERE rel.amount > 0)
RETURN a.account_id AS anchor, length(p) AS hops, relationships(p) AS edges
LIMIT 50;

// D1. Short loops (2..5 hops) returning to same account
MATCH p = (a:Account)-[:SENT*2..5]->(a)
WHERE ALL(rel IN r WHERE rel.amount > 0 AND rel.timestamp >= datetime() - duration('P7D'))
RETURN a.account_id AS anchor, length(p) AS hops, r
LIMIT 50;

// D2. Fan-in/Fan-out anomalies
MATCH (a:Account)<-[t:SENT]-()
WITH a, count(t) AS fan_in, sum(t.amount) AS in_amt
ORDER BY fan_in DESC
LIMIT 50
RETURN a.account_id AS account, fan_in, in_amt;

// D3. Multi-identifier linkage (phone OR ip OR device OR email)
MATCH (p:Person)
OPTIONAL MATCH (p)-[:USES_PHONE]->(ph:Phone)
OPTIONAL MATCH (p)-[:LOGGED_FROM]->(ip:IP)
OPTIONAL MATCH (p)-[:USES_DEVICE]->(d:Device)
OPTIONAL MATCH (p)-[:USES_EMAIL]->(e:Email)
RETURN p.person_id AS person,
       collect(DISTINCT ph.number)  AS phones,
       collect(DISTINCT ip.value)   AS ips,
       collect(DISTINCT d.device_id) AS devices,
       collect(DISTINCT e.address)  AS emails
LIMIT 100;

// D4. Simple risk score (rule-based starter)
// Assumptions:
// - Risk +2 if person shares Phone with >=1 others
// - Risk +3 if person shares IP with >=1 others
// - Risk +1 per distinct short loop touching their accounts (coarse)
// Customize freely.
CALL {
  WITH 1 AS dummy
  MATCH (p:Person)
  OPTIONAL MATCH (p)-[:USES_PHONE]->(ph:Phone)<-[:USES_PHONE]-(other:Person)
  WITH p, count(DISTINCT other) AS phone_shares
  OPTIONAL MATCH (p)-[:LOGGED_FROM]->(ip:IP)<-[:LOGGED_FROM]-(other2:Person)
  WITH p, phone_shares, count(DISTINCT other2) AS ip_shares
  OPTIONAL MATCH (p)-[:OWNS]->(a:Account)
  OPTIONAL MATCH pLoop = (a)-[:SENT_TO*2..5]->(a)
  WITH p, phone_shares, ip_shares, count(pLoop) AS loops
  RETURN p.person_id AS person,
         (CASE WHEN phone_shares>0 THEN 2 ELSE 0 END) +
         (CASE WHEN ip_shares>0 THEN 3 ELSE 0 END) +
         (CASE WHEN loops>0 THEN loops ELSE 0 END) AS risk
}
RETURN person, risk
ORDER BY risk DESC
LIMIT 50;


/* ===========================
   E. MAINTENANCE / UTILITIES
   =========================== */

// E1. Example upsert guards (pattern)
MERGE (ip:IP {value: "203.0.113.10"})
ON CREATE SET ip.first_seen = datetime(), ip.last_seen = datetime()
ON MATCH  SET ip.last_seen  = datetime();

// E2. (Careful) duplicate rel cleanup example: remove duplicate SENT_TO edges with same (src,dst,txn_id)
MATCH (a1:Account)-[r:SENT_TO]->(a2:Account)
WITH a1, a2, r.txn_id AS txn_id, collect(r) AS rels
WHERE txn_id IS NOT NULL AND size(rels) > 1
FOREACH (r IN rels[1..] | DELETE r);