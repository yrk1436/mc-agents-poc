from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict
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

# Load available brands and surveys
generator = SurveyDataGenerator()
AVAILABLE_BRANDS = generator.brands
AVAILABLE_SURVEYS = generator.surveys

class QuestionRequest(BaseModel):
    question: str
    user_id: str  # Required for user context
    thread_id: str  # Required for thread context
    brand_id: str  # Required for data filtering
    survey_id: str  # Required for data filtering
    context: Optional[dict] = {}  # Additional context for this specific question

class QuestionResponse(BaseModel):
    response: dict
    follow_up_suggestions: Optional[List[str]] = None

class AvailableDataResponse(BaseModel):
    brands: Dict[str, List[str]]

@app.get("/available_data", response_model=AvailableDataResponse)
async def get_available_data():
    """Get list of available brands and their surveys"""
    return {
        "brands": {
            brand: list(surveys.keys())
            for brand, surveys in AVAILABLE_SURVEYS.items()
        }
    }

@app.post("/process_question", response_model=QuestionResponse)
async def process_question(request: QuestionRequest):
    try:
        # Validate brand_id and survey_id
        if request.brand_id not in AVAILABLE_BRANDS:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid brand_id. Available brands: {AVAILABLE_BRANDS}"
            )
        
        if request.brand_id not in AVAILABLE_SURVEYS:
            raise HTTPException(
                status_code=400,
                detail=f"No surveys found for brand: {request.brand_id}"
            )
            
        if request.survey_id not in AVAILABLE_SURVEYS[request.brand_id]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid survey_id for brand {request.brand_id}. Available surveys: {list(AVAILABLE_SURVEYS[request.brand_id].keys())}"
            )
        
        # Get context for this interaction
        context = context_manager.get_context(request.user_id, request.thread_id)
        
        # Add brand and survey context
        context.update({
            "brand_id": request.brand_id,
            "survey_id": request.survey_id
        })
        
        # Process question through agents
        response = agents.process_question(
            question=request.question,
            context=context,
            thread_id=request.thread_id,
            user_id=request.user_id
        )
        
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
