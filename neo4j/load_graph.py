from neo4j import GraphDatabase
from dotenv import load_dotenv
import os, sys

load_dotenv()

NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USER     = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DB       = os.getenv("NEO4J_DATABASE", None)  # e.g., "neo4j" on Aura; None uses default

# Usage:
#   python load_graph.py                  -> runs neo4j/load.cypher
#   python load_graph.py neo4j/queries.cypher
cypher_script_path = sys.argv[1] if len(sys.argv) > 1 else "neo4j/load.cypher"

# Optional: stop on first error (set STOP_ON_ERROR=true in .env to enable)
STOP_ON_ERROR = os.getenv("STOP_ON_ERROR", "false").lower() == "true"

def iter_commands_from_file(file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        text = f.read()
    # Split by semicolon; ignore blanks and comment-only fragments
    parts = [p.strip() for p in text.split(";")]
    for p in parts:
        if not p:
            continue
        # Skip fragments that are only comments/whitespace
        lines = [ln for ln in p.splitlines() if ln.strip() and not ln.strip().startswith("//")]
        if not lines:
            continue
        yield "\n".join(lines)

def run_command(session, cmd: str, idx: int):
    print(f"\n🔹 Executing command {idx} (first 120 chars):\n{cmd[:120]}...\n")
    # Use a write transaction for safety with MERGE/LOAD CSV
    def _tx_run(tx):
        tx.run(cmd)
    session.execute_write(_tx_run)
    print("✅ Success.")

def main():
    if not all([NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD]):
        print("❌ Missing Neo4j environment variables. Please check your .env file.")
        return

    print(f"🌐 Connecting to {NEO4J_URI} as {NEO4J_USER}")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        # Choose DB if provided (Aura often uses 'neo4j')
        if NEO4J_DB:
            session_args = {"database": NEO4J_DB}
        else:
            session_args = {}

        with driver.session(**session_args) as session:
            for i, cmd in enumerate(iter_commands_from_file(cypher_script_path), start=1):
                try:
                    run_command(session, cmd, i)
                except Exception as e:
                    print(f"❗ Error in command {i}:\n{e}")
                    if STOP_ON_ERROR:
                        print("⛔ Stopping due to STOP_ON_ERROR.")
                        break
        print("\n✅ All commands processed.")
    finally:
        driver.close()

if __name__ == "__main__":
    main()