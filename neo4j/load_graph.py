from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

cypher_script_path = "neo4j/load.cypher"  # Adjust path if needed

def execute_cypher_file(driver, file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        cypher_commands = file.read().split(";")

        with driver.session() as session:
            for i, command in enumerate(cypher_commands):
                cmd = command.strip()
                if cmd:
                    try:
                        print(f"\n🔹 Executing command {i+1}:\n{cmd[:120]}...")
                        session.run(cmd)
                        print("Success.")
                    except Exception as e:
                        print(f"Error in command {i+1}:\n{cmd[:120]}...\n{e}")

# Connect and execute
if all([NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD]):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver:
        print(f"🌐 Connecting to {NEO4J_URI} as {NEO4J_USER}")
        execute_cypher_file(driver, cypher_script_path)
        print("\nAll commands executed.")
else:
    print("Missing Neo4j environment variables. Please check your .env file.")