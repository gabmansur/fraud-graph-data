# Fraud Graph Demo
A lightweight, high-performance pipeline for graph-based fraud detection, clean, local, and focused on transparency.  
This is a research prototype designed for exploration, built around explainability and creative thinking.

## Why this project?

In financial transactions, fraud rarely acts alone. It moves through *networks*.

This prototype transforms transactional data into graph structures to surface:
- Suspicious links between users or accounts
- Identity reuse (phones, IPs, companies)
- Fraud rings and dormant "mule" accounts

It’s part of my personal portfolio to showcase:

- Graph thinking applied to fraud analytics
- Enriching raw data into typed entities and relationships
- Neo4j as a tool for intuitive fraud investigation
- Modular, interpretable code designed for insight

## Folder Structure
```
fraud-graph-demo/
│
├── assets/
│ ├── pipeline_flow.mmd # mermaid diagram
│
├── data/
│ ├── raw/ # Original input file (from Kaggle)
│ └── processed/ # All generated node + edge CSVs
│
├── notebooks/ # Data exploration + export scripts
│ ├── 01_generate_metadata.ipynb
│ ├── 02_inject_fraud_patterns.ipynb
│ └── 03_export_for_neo4j.ipynb
│
├── neo4j/ # Cypher scripts to load + query graph
│ ├── load.cypher
│ └── queries.cypher
│
├── scripts/ # Same content as notebooks but as .py scripts
│
├── streamlit_app/ (WIP/planned)
├── requirements.txt
└── README.md
```

## About the dataset

This project is based on the [Kaggle PaySim Dataset](https://www.kaggle.com/datasets/ealaxi/paysim1), a simulation of mobile money transactions — great for prototyping fraud detection logic.

> ⚠️ **Note**: The raw file `PS_20174392719_1491204439457_log.csv` (~470MB) cannot be committed to GitHub due to size limits.  
> To run the pipeline, download it manually from the Kaggle link above and place it in:  
> `data/raw/`

## Pipeline Flow

1. **Raw Input**  
   Load mobile transaction logs from `data/raw/`

2. **Metadata Generation**  
   Extract typed nodes and relationships from transactional data:
   - People, phones, IPs, companies
   - Links like `sent`, `logged_from`, `uses_phone`, `works_for`

3. **Fraud Pattern Injection (Optional)**  
   Inject known suspicious behaviors (e.g. shared phones, fake orgs)

4. **Export for Neo4j**  
   Convert enriched entities into CSVs and load into Neo4j via `load.cypher`

5. **Graph Exploration**  
   Use `queries.cypher` to surface suspicious clusters and behavior


![Pipeline Diagram](assets/pipeline_flow.png)


This project doesn’t rely on black-box ML, it’s **transparent, interpretable, and built for real human investigation**.

We inject known behavioral patterns to simulate realistic threats:

- **Phone Reuse**  
  A single phone number tied to multiple user accounts or identities, a classic fraudster trick.

- **Circular Transfers**  
  Funds looping between accounts (e.g., A → B → A) to obscure traceability and simulate legitimacy.

- **Fake Employers**  
  Multiple users linked to the same fake company, often used to justify income or create trust anchors.

- **Receiver-Only Accounts**  
  Dormant accounts that suddenly come alive just to receive funds, often acting as *mules* in fraud rings.

> All of these behaviors are encoded as graph structures, easy to trace visually or query using Cypher in Neo4j.


## Setup & Usage

1. **Clone the repo and create your environment**
```bash
git clone https://github.com/your-user/fraud-graph-demo
cd fraud-graph-demo
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

2. **Open the Notebooks**
The core logic lives inside three clean, easy-to-follow notebooks

| Notebook                         | Purpose                                                                                                                  |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `01_generate_metadata.ipynb`     | Cleans the raw transaction file and enriches it with metadata like unique phone numbers, company IDs, and user profiles. |
| `02_inject_fraud_patterns.ipynb` | Injects synthetic but realistic fraud patterns into the graph (e.g. loops, reused devices, mule accounts).               |
| `03_export_for_neo4j.ipynb`      | Converts the graph into CSV files compatible with Neo4j’s `LOAD CSV` Cypher command.                                     |

Just run them in order. Each cell is commented and modular.

## 3. **Launch Neo4j (Optional)**
If you want to visualize the graph:

- Start Neo4j locally (you can use Neo4j Desktop or Docker)
- Use the neo4j/load.cypher script to ingest the graph
- Explore patterns visually or run predefined queries in neo4j/queries.cypher

## What makes this project cool?

- Clean, local Python setup, no cloud dependencies
- Uses Spark for scale, but small enough to run on a laptop
- Graph-based fraud detection logic
- Full Pytest test coverage
- Modular, well-commented codebase

## Roadmap

- Add machine learning fraud classifiers
- Integrate with Neo4j for more complex graph querying
- Create interactive dashboard (e.g. Streamlit or Gradio)
- Setup LFS for easier dataset sharing

## FAQ

**Q: Where’s the full dataset?**  
A: Too big for GitHub! You’ll find the download link and setup info above.

**Q: Why use Spark locally?**  
A: It’s efficient, parallel, and you’ll thank me if you scale this later.

**Q: Can I use this as a base for my own fraud detection project?**  
A: Yes! Just credit me (and the original dataset authors) if you publish it.

**Q: Are Tom and Mia involved?**  
A: They supervised the testing phase and provided purring QA support. Also if you see typos or strange stuff, it's their keyboard walking routine.

## From the heart

This project reflects how I like to work: clean, smart, curious, and creative.  
If you’re hiring or collaborating, I’d love to talk.

— Gabi
