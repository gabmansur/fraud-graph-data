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
     C1. Phase 1 - Map the Full Network
     C2. Phase 2 - Check for Phone Overlap
     C3. Phase 3 - Check for Phone + IP overlap
     C4. Phase 4 - Ring Detection & Case Prioritization

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

// A1. Constraints (idempotent) – align with load.cypher
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person)  REQUIRE p.person_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (ph:Phone)  REQUIRE ph.number    IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (ip:IP)     REQUIRE ip.value     IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Company) REQUIRE c.name       IS UNIQUE;

// A2. Helpful indexes
CREATE INDEX IF NOT EXISTS FOR (p:Person) ON (p.name);
CREATE INDEX IF NOT EXISTS FOR (a:Account) ON (a.iban);
CREATE INDEX IF NOT EXISTS FOR (tx:Txn) ON (tx.timestamp);
CREATE INDEX IF NOT EXISTS FOR (ip:IP) ON (ip.last_seen);


/* ===========================
   B. SANITY & EXPLORATION
   =========================== */

// B1. Counts by label (explicit)
RETURN
  (MATCH (p:Person)  RETURN count(p))  AS persons,
  (MATCH (ph:Phone)  RETURN count(ph)) AS phones,
  (MATCH (ip:IP)     RETURN count(ip)) AS ips,
  (MATCH (c:Company) RETURN count(c))  AS companies;

// B2. Counts by relationship type (explicit)
RETURN
  (MATCH (:Person)-[r:SENT]->(:Person)        RETURN count(r)) AS sent_edges,
  (MATCH (:Person)-[r:USES_PHONE]->(:Phone)   RETURN count(r)) AS phone_edges,
  (MATCH (:Person)-[r:LOGGED_FROM]->(:IP)     RETURN count(r)) AS ip_edges,
  (MATCH (:Person)-[r:WORKS_FOR]->(:Company)  RETURN count(r)) AS works_for_edges;


// B3. Top-degree People (hubs)
MATCH (p:Person)
RETURN p.person_id AS person, COUNT { (p)--() } AS deg
ORDER BY deg DESC
LIMIT 20;


/* ===========================
   C. PHASE QUERIES
   =========================== */

// C1. Phase 1 - Map the Full Network
MATCH (n)-[r]->(m)
RETURN n, r, m
LIMIT 5000

// C2. Phase 2 - Check for Phone Overlap
MATCH (p1:Person)-[r1:USES_PHONE]->(ph:Phone)<-[r2:USES_PHONE]-(p2:Person)
WHERE p1 <> p2
RETURN p1, p2, ph, r1, r2

// C3. Phase 3 - Check for Phone + IP overlap
MATCH (p:Person)-[:USES_PHONE]->(ph:Phone),
      (p)-[:LOGGED_FROM]->(ip:IP)
WITH ph.number AS phone,
     ip.value  AS ip,
     collect(DISTINCT p.person_id) AS people,
     count(DISTINCT p) AS cnt
WHERE phone IS NOT NULL AND phone <> ''
  AND ip    IS NOT NULL AND ip    <> ''
  AND cnt > 1
RETURN phone, ip, cnt AS count, people
ORDER BY count DESC
LIMIT 5;

// C4. Phase 4 - Ring Detection & Case Prioritization
// Phone+IP rings = groups of people sharing the same phone AND IP

//Ring stats (total, largest, smallest, average)
MATCH (p:Person)-[:USES_PHONE]->(ph:Phone),
      (p)-[:LOGGED_FROM]->(ip:IP)
WITH ph.number AS phone, ip.value AS ip, collect(DISTINCT p) AS people
WHERE phone IS NOT NULL AND phone <> ''
  AND ip    IS NOT NULL AND ip    <> ''
  AND size(people) > 1
WITH size(people) AS ring_size
RETURN count(*)            AS total_rings,
       max(ring_size)      AS largest_ring,
       min(ring_size)      AS smallest_ring,
       round(avg(ring_size), 2) AS avg_ring_size;

// Multi-ring members (how many people appear in more than one ring)
MATCH (p:Person)-[:USES_PHONE]->(ph:Phone),
      (p)-[:LOGGED_FROM]->(ip:IP)
WITH p, ph.number AS phone, ip.value AS ip
WHERE phone IS NOT NULL AND phone <> ''
  AND ip    IS NOT NULL AND ip    <> ''
WITH p, collect(DISTINCT phone + '|' + ip) AS combos
WHERE size(combos) > 1
RETURN count(*) AS multi_ring_members;

// to see who they are, run:
MATCH (p:Person)-[:USES_PHONE]->(ph:Phone),
      (p)-[:LOGGED_FROM]->(ip:IP)
WITH p, ph.number AS phone, ip.value AS ip
WHERE phone IS NOT NULL AND phone <> ''
  AND ip    IS NOT NULL AND ip    <> ''
WITH p, collect(DISTINCT phone + '|' + ip) AS combos
WHERE size(combos) > 1
RETURN coalesce(p.person_id, elementId(p)) AS person,
       size(combos) AS rings_involved,
       combos
ORDER BY rings_involved DESC, person
LIMIT 25;

// Ring size distribution (for a quick histogram-style view)
MATCH (p:Person)-[:USES_PHONE]->(ph:Phone),
      (p)-[:LOGGED_FROM]->(ip:IP)
WITH ph.number AS phone, ip.value AS ip, collect(DISTINCT p) AS people
WHERE phone IS NOT NULL AND phone <> ''
  AND ip    IS NOT NULL AND ip    <> ''
  AND size(people) > 1
WITH size(people) AS ring_size
RETURN ring_size, count(*) AS rings
ORDER BY ring_size DESC;

// The ring list itself (to sanity-check the counts)
MATCH (p:Person)-[:USES_PHONE]->(ph:Phone),
      (p)-[:LOGGED_FROM]->(ip:IP)
WITH ph.number AS phone, ip.value AS ip, collect(DISTINCT p) AS people
WHERE phone IS NOT NULL AND phone <> ''
  AND ip    IS NOT NULL AND ip    <> ''
  AND size(people) > 1
RETURN phone,
       ip,
       size(people) AS ring_size,
       [x IN people | coalesce(x.person_id, elementId(x))] AS members
ORDER BY ring_size DESC, phone, ip;


/* ===========================
   D. FRAUD PATTERNS & SCORES
   =========================== */

/*

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

*/

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