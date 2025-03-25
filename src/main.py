from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from loguru import logger
import uvicorn
from pathlib import Path
import os
from dotenv import load_dotenv

from agents.agents import MarketResearchAgents
from utils.context_manager import ContextManager
from data.generator import SurveyDataGenerator

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(title="Market Research Insights API")

# Initialize components
context_manager = ContextManager()
agents = MarketResearchAgents()

class QuestionRequest(BaseModel):
    user_id: str
    thread_id: str
    brand_id: str
    survey_id: str
    question: str

class QuestionResponse(BaseModel):
    response: dict
    follow_up_suggestions: Optional[List[str]] = None

@app.post("/process_question", response_model=QuestionResponse)
async def process_question(request: QuestionRequest):
    try:
        # Get context for this interaction
        context = context_manager.get_context(request.user_id, request.thread_id)
        
        # Process question through agents
        response = agents.process_question(request.question, context)
        
        # Generate follow-up suggestions if needed
        follow_ups = []
        if "vague" in response.get("question_type", "").lower():
            follow_ups = [
                "Would you like to know about response rates?",
                "Should we analyze demographic breakdowns?",
                "Would you like to see key findings from specific questions?"
            ]
        
        # Update context with this interaction
        context_manager.update_interaction(
            request.user_id,
            request.thread_id,
            request.question,
            str(response)
        )
        
        return QuestionResponse(
            response=response,
            follow_up_suggestions=follow_ups if follow_ups else None
        )
        
    except Exception as e:
        logger.error(f"Error processing question: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("startup")
async def startup_event():
    # Ensure data directory exists
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Generate sample data if it doesn't exist
    if not (data_dir / "survey_responses.parquet").exists():
        logger.info("Generating sample survey data...")
        generator = SurveyDataGenerator()
        generator.save_to_parquet()
        logger.info("Sample data generated successfully")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
