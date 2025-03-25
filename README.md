# Market Research AI Agents POC

A proof-of-concept system for analyzing market research survey data using AI agents.

## Overview

This project demonstrates a multi-agent AI system that can process natural language questions about market research data. It uses CrewAI to orchestrate different agent types:

- **Router Agent**: Determines the type of question being asked
- **SQL Agent**: Generates SQL queries for data analysis
- **Insights Agent**: Provides qualitative analysis and recommendations

## Features

- Natural language processing of market research questions
- SQL query generation and execution for data analysis
- Contextual conversation management
- Support for different question types (analytical, insight-based, hybrid)
- Follow-up suggestion generation

## Tech Stack

- FastAPI
- CrewAI
- LangChain
- DuckDB
- LangSmith (optional)

## Setup

1. Clone the repository
2. Create a virtual environment: `python -m venv .venv`
3. Activate the virtual environment:
   - Windows: `.venv\Scripts\activate`
   - Unix/MacOS: `source .venv/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`
5. Create a `.env` file with your API keys

## Usage

1. Start the API server: `python src/main.py`
2. Send questions via the `/process_question` endpoint

## Project Structure

- `src/`: Main source code
  - `agents/`: Agent definitions and logic
  - `data/`: Data handling and generation
  - `utils/`: Utility classes and functions
- `tests/`: Test cases
- `data/`: Sample data storage 