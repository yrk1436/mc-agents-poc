from crewai import Agent, Task, Crew, Process
from langchain_openai import ChatOpenAI
from typing import List, Dict, Optional
import duckdb
import chromadb
from loguru import logger
from langsmith import Client
from langsmith.run_helpers import traceable
from utils.db_manager import DatabaseManager
from utils.chat_context import ChatContextManager
import re

class MarketResearchAgents:
    def __init__(self):
        self.llm = ChatOpenAI(
            model="gpt-4o-mini",
            temperature=0.7
        )
        
        # Initialize database and chat context managers
        self.db_manager = DatabaseManager()
        self.chat_context = ChatContextManager()
        
        self.router_agent = Agent(
            role='Query Router',
            goal='Accurately classify questions as analytical, insight-based, or hybrid',
            backstory="""You're an expert at understanding the intent behind market research questions.
            You classify each question as either:
            - 'analytical' (requires SQL/data analysis)
            - 'insight' (requires qualitative analysis)
            - 'hybrid' (requires both)""",
            verbose=True,
            allow_delegation=False,
            llm=self.llm
        )
        
        self.sql_agent = Agent(
            role="Data Analyst",
            goal="Generate and execute SQL queries for survey data analysis",
            backstory="""I specialize in analyzing survey response data with a flattened structure where:
            - Each row represents a single question response
            - Columns include:
              * Survey Response Metadata:
                - response_id: unique ID for each question response
                - user_id: ID of the person who took the survey
                - brand_id: brand being surveyed
                - survey_id: specific survey name
                - timestamp: when the response was recorded
              * Question info: 
                - question_id: unique identifier for the question
                - question_type: type of question (rating, multiple_choice, open_ended, scale)
                - question_text: the actual question text
                - question_group: grouping/category of the question
              * Answer: stored as string, with different types:
                - For 'rating' or 'scale' questions: numeric values that need CAST/TRY_CAST
                - For 'multiple_choice': text values that should not be cast
                - For 'open_ended': text responses that should not be cast
              * Question metadata: scale_min/max for ratings (as strings)
              * Demographics: 
                - age: stored as string
                - gender: 'Male', 'Female', or 'Other' (case-sensitive)
                - location: respondent's location
                - income_bracket: respondent's income bracket
                - education: respondent's education level
                
            Key SQL Tips:
            - Always check question_type before casting answers
            - Use TRY_CAST for numeric operations on ratings/scales
            - For aggregations, separate numeric and text responses
            - Handle NULL values appropriately""",
            verbose=True,
            allow_delegation=False,
            llm=self.llm
        )
        
        self.insights_agent = Agent(
            role="Insights Specialist",
            goal="Extract meaningful patterns and insights from survey responses",
            backstory="""I excel at:
            - Analyzing open-ended responses for themes
            - Comparing ratings across demographics
            - Identifying trends in customer satisfaction
            - Providing actionable recommendations based on data""",
            verbose=True,
            allow_delegation=False,
            llm=self.llm
        )

    def create_router_task(self, question: str, context: dict) -> Task:
        return Task(
            description=f"""
            Analyze this market research question and determine if it requires:
            1. Analytical processing (SQL queries for data analysis)
            2. Insight generation (pattern analysis and recommendations)
            3. Both (hybrid approach)
            
            Question: {question}
            Context: {context}
            """,
            expected_output="Return ONLY ONE of these exact strings: 'analytical', 'insight', or 'hybrid'",
            agent=self.router_agent
        )

    def create_sql_task(self, question: str, context: dict) -> Task:
        # Get database schema info for better query generation
        schema_info = self.db_manager.get_schema_info()
        
        return Task(
            description=f"""
            Generate a SQL query to answer this analytical question:
            Question: {question}
            Context: {context}
            
            Available Views:
            {schema_info}
            
            The data structure in parquet files is flattened with these columns:
            - Metadata:
              * response_id: unique ID for each question response
              * user_id: ID of the survey respondent
              * brand_id: brand being surveyed
              * survey_id: specific survey name
              * timestamp: when the response was recorded
            
            - Question Information:
              * question_id: unique identifier for the question
              * question_type: type of question (rating, multiple_choice, open_ended, scale)
              * question_text: the actual question text
              * question_group: grouping/category of the question
              * answer: the response value (stored as string)
              * scale_min, scale_max: for rating/scale questions
              * options: pipe-separated list of choices for multiple_choice questions
            
            - Demographics:
              * age: respondent's age (stored as string)
              * gender: respondent's gender
              * location: respondent's location
              * income_bracket: respondent's income bracket
              * education: respondent's education level
            
            Remember:
            - Numeric values (age, answer for ratings) are stored as strings and need CAST
            - Multiple choice options are stored as pipe-separated strings
            """,
            expected_output="""Return your response in this exact format:
            ```sql
            YOUR_SQL_QUERY_HERE
            ```
            
            Followed by a brief explanation of what the query does.""",
            agent=self.sql_agent
        )

    def create_insights_task(self, question: str, context: dict) -> Task:
        return Task(
            description=f"""
            Analyze this question and provide meaningful insights:
            Question: {question}
            Context: {context}
            
            Focus on:
            - Patterns in the data
            - Demographic correlations
            - Actionable recommendations
            - Key findings and their implications
            """,
            expected_output="A detailed analysis with clear insights and recommendations",
            agent=self.insights_agent
        )

    def process_question(self, question: str, context: dict, thread_id: Optional[str] = None, user_id: Optional[str] = None) -> dict:
        """Process a question and maintain chat context if thread_id is provided."""
        # If thread_id is provided, get or initialize context
        if thread_id and user_id:
            saved_context = self.chat_context.get_context(thread_id)
            if saved_context:
                # Merge saved context with current context
                context.update(saved_context["context"])
        
        # Start with router task
        router_task = self.create_router_task(question, context)
        crew = Crew(
            agents=[self.router_agent],
            tasks=[router_task],
            verbose=True,
            process=Process.sequential
        )
        question_type = crew.kickoff()
        
        results = []
        if "analytical" in str(question_type).lower():
            sql_task = self.create_sql_task(question, context)
            crew = Crew(
                agents=[self.sql_agent],
                tasks=[sql_task],
                verbose=True,
                process=Process.sequential
            )
            sql_response = crew.kickoff()
            
            # Extract SQL query from agent's response            
            sql_match = re.search(r"```sql\n(.*?)\n```", str(sql_response), re.DOTALL)
            if sql_match:
                sql_query = sql_match.group(1)
                sql_results = self.execute_sql_analysis(sql_query)
                results.append({
                    "type": "analytical",
                    "agent_response": str(sql_response),
                    "sql_results": sql_results
                })
            else:
                results.append({
                    "type": "analytical",
                    "agent_response": str(sql_response),
                    "error": "No SQL query found in response"
                })
                
        elif "insight" in str(question_type).lower():
            insight_task = self.create_insights_task(question, context)
            crew = Crew(
                agents=[self.insights_agent],
                tasks=[insight_task],
                verbose=True,
                process=Process.sequential
            )
            insight_response = crew.kickoff()
            results.append({
                "type": "insight",
                "analysis": str(insight_response)
            })
            
        else:  # Hybrid
            # Execute SQL analysis first
            sql_task = self.create_sql_task(question, context)
            crew = Crew(
                agents=[self.sql_agent],
                tasks=[sql_task],
                verbose=True,
                process=Process.sequential
            )
            sql_response = crew.kickoff()
            
            sql_match = re.search(r"```sql\n(.*?)\n```", str(sql_response), re.DOTALL)
            if sql_match:
                sql_query = sql_match.group(1)
                sql_results = self.execute_sql_analysis(sql_query)
                results.append({
                    "type": "analytical",
                    "agent_response": str(sql_response),
                    "sql_results": sql_results
                })
            
            # Then get insights
            insight_task = self.create_insights_task(question, context)
            crew = Crew(
                agents=[self.insights_agent],
                tasks=[insight_task],
                verbose=True,
                process=Process.sequential
            )
            insight_response = crew.kickoff()
            results.append({
                "type": "insight",
                "analysis": str(insight_response)
            })

        response = {
            "question_type": str(question_type),
            "results": results
        }

        # Save updated context if thread_id is provided
        if thread_id and user_id:
            context["last_question"] = question
            context["last_response"] = response
            self.chat_context.save_context(thread_id, user_id, context)

        return response

    def execute_sql_analysis(self, query: str) -> List[Dict]:
        """Execute a SQL query and return results"""
        try:
            return self.db_manager.execute_query(query)
        except Exception as e:
            logger.error(f"Error executing SQL query: {str(e)}\nQuery: {query}")
            return [{"error": str(e)}]

    def __del__(self):
        """Cleanup database connection"""
        if hasattr(self, 'db_manager'):
            self.db_manager.close()
