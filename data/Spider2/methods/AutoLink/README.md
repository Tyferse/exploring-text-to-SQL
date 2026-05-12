# AutoLink: Autonomous Schema Exploration and Expansion for Scalable Schema Linking in Text-to-SQL at Scale

<p align="center">
| <a href="https://arxiv.org/abs/2511.17190"><b>arXiv</b></a> |
</p>

## Overview
![AutoLink](assets/overview.png)

For industrial-scale text-to-SQL, supplying the entire database schema to Large Language Models (LLMs) is impractical due to context window limits and irrelevant noise. Schema linking, which filters the schema to a relevant subset, is therefore critical. However, existing methods incur prohibitive costs, struggle to trade off recall and noise, and scale poorly to large databases. We present **AutoLink**, an autonomous agent framework that reformulates schema linking as an iterative, agent-driven process. Guided by an LLM, AutoLink dynamically explores and expands the linked schema subset, progressively identifying necessary schema components without inputting the full database schema. Our experiments demonstrate AutoLink's superior performance, achieving state-of-the-art strict schema linking recall of **97.4%** on Bird-Dev and **91.2%** on Spider-2.0-Lite, with competitive execution accuracy, i.e., **68.7** EX on Bird-Dev (better than CHESS) and **34.9** EX on Spider-2.0-Lite (ranking 2nd on the official leaderboard). Crucially, AutoLink exhibits **exceptional scalability**, **maintaining high recall**, **efficient token consumption**, and **robust execution accuracy** on large schemas (e.g., over 3,000 columns) where existing methods severely degrade—making it a highly scalable, high-recall schema linking solution for industrial text-to-SQL systems.

## Folder Structure  
```
- linking_results/                     -- Results after schema linking used to SQL generation
- run/  
  - bigquery_credentials/              -- Place bigquery credentials
  - documents/                         -- Constructed column-level documents
  - embeddings/                        -- Document embeddings
  - log_path/                          -- Results of schema linking and sql generation
    - final_schema_prompts/            -- Prompts after AutoLink's schema linking process used to SQL generation
    - schema_prompts/                  -- Prompts of schema used to AutoLink's schema linking process
    - sql_gen/                         -- Candidate SQLs and execution results
    - sql_revise/                      -- Revision result of every candidates
    - sql_selection/                   -- The final SQL after cadidate selection
    - merge_candidates.json            -- The final schema linking results, used for calculating recall(The difference from unfilled_schema.json lies in whether nested columns in BigQuery are expanded)
    - unfilled_schema.json             -- The final schema linking results, used for constructing the SQL generation prompt
    - ...                              -- Other intermediate results
  - resource/                          -- Copied from Spider 2.0-Lite Repo
  - snowflake_credential/              -- Place snowflake credential
  - add_id.py                          -- Primary and foreign key rule processing
  - complete_schema.py                 -- Iterative, agent-driven schema linking
  - config.py                          -- Prompts 
  - embedding_docs.py                  -- Embedding documents
  - generate_docs.py                   -- Generate documents  
  - generate_schema.py                 -- Generate schema  
  - main.sh                            -- Main script of schema linking
  - model_manager.py                   -- Embedding model manager
  - postprocess.py                     -- Postprocess after schema linking
  - retrieve_topk_schema.py            -- Retrieve script
  - spdier2_data.json                  -- Spider 2.0-Lite test set
  - sql_execution.py                   -- Execute SQL
  - sql_gen.sh                         -- The script to generate SQL
  - sql_generation.py                  -- Generate candidate SQLs
  - sql_revise.py                      -- Revise the SQL with error execution after candidate SQL generation
  - sql_selection.py                   -- Select the final SQL via self-consistency based on SQL execution results
  - utils.py                           -- Utility functions
```

## Settint Up Environment
1. Clone the repository:
  ```bash
  git clone https://github.com/wzy416/AutoLink.git
  cd AutoLink
  ```
2. Create conda environment:
  ```bash
  conda create -n AutoLink python=3.12
  conda activate AutoLink
  pip install -r requirements.txt
  ```
3. Modify the api key:

  To use your own LLM, modify the OPENAI_API_KEY and OPENAI_BASE_URL in `run/main.sh`.


4. Copy repository from [`Spider 2.0-lite`](https://github.com/xlang-ai/Spider2/tree/main/spider2-lite)

  You should copy [`spdier2-lite/resource`](https://github.com/xlang-ai/Spider2/tree/main/spider2-lite) from [`Spider 2.0`](https://github.com/xlang-ai/Spider2) repo to `run/resource/`. Follow the [`bigquery guideline`](https://github.com/xlang-ai/Spider2/blob/main/assets/Bigquery_Guideline.md) and  [`snowflake guideline`](https://github.com/xlang-ai/Spider2/blob/main/assets/Snowflake_Guideline.md) to sign up accounts, then put the credential json files under `run/bigquery_credentials/` and `run/snowflake_credential/` separately.

## Running the code
1. Schema Linking
  ```bash
  cd ./run
  bash main.sh
  ```
  It will gradually complete the entire process from document construction, document embedding, initial schema retrieval, to schema exploration and expansion.

2. SQL Generation
  ```bash
  bash sql_gen.sh
  ```
  It will progressively complete candidate SQL generation, SQL revision, and final SQL selection.

# Citation
If you find this repo helpful, please cite our work:
```bibtex
@misc{wang2025autolinkautonomousschemaexploration,
      title={AutoLink: Autonomous Schema Exploration and Expansion for Scalable Schema Linking in Text-to-SQL at Scale}, 
      author={Ziyang Wang and Yuanlei Zheng and Zhenbiao Cao and Xiaojin Zhang and Zhongyu Wei and Pei Fu and Zhenbo Luo and Wei Chen and Xiang Bai},
      year={2025},
      eprint={2511.17190},
      archivePrefix={arXiv},
      primaryClass={cs.CL},
      url={https://arxiv.org/abs/2511.17190}, 
}
```



