import pytest
from pathlib import Path
import uuid
from src.agents.agents import MarketResearchAgents
from src.utils.context_manager import ContextManager
from src.data.generator import SurveyDataGenerator

@pytest.fixture(scope="session")
def sample_data():
    """Generate sample data for testing"""
    data_dir = Path("test_data")
    data_dir.mkdir(exist_ok=True)
    
    generator = SurveyDataGenerator()
    generator.save_to_parquet(str(data_dir))
    
    return data_dir

@pytest.fixture
def context_manager():
    """Create a test context manager"""
    return ContextManager(storage_dir="test_data/context")

@pytest.fixture
def agents():
    """Create market research agents"""
    return MarketResearchAgents()

def test_analytical_question(agents, context_manager, sample_data):
    user_id = str(uuid.uuid4())
    thread_id = str(uuid.uuid4())
    context = context_manager.get_context(user_id, thread_id)
    
    question = "How many male respondents rated TechCorp's customer service as Excellent?"
    response = agents.process_question(question, context)
    
    assert response["question_type"].lower().startswith("analytical")
    assert len(response["results"]) >= 1
    assert "SQL" in str(response["results"][0])

def test_insight_question(agents, context_manager, sample_data):
    user_id = str(uuid.uuid4())
    thread_id = str(uuid.uuid4())
    context = context_manager.get_context(user_id, thread_id)
    
    question = "What themes are emerging from the open-ended feedback for EcoGoods?"
    response = agents.process_question(question, context)
    
    assert "insight" in response["question_type"].lower()
    assert len(response["results"]) >= 1
    assert "theme" in str(response["results"][0]).lower()

def test_hybrid_question(agents, context_manager, sample_data):
    user_id = str(uuid.uuid4())
    thread_id = str(uuid.uuid4())
    context = context_manager.get_context(user_id, thread_id)
    
    question = "Are male respondents more satisfied with HealthPlus than female respondents?"
    response = agents.process_question(question, context)
    
    assert "hybrid" in response["question_type"].lower()
    assert len(response["results"]) >= 2

def test_context_persistence(context_manager):
    user_id = str(uuid.uuid4())
    thread_id = str(uuid.uuid4())
    
    # Save some context
    context_manager.save_user_context(user_id, {"preference": "detailed_analysis"})
    context_manager.save_thread_context(thread_id, {"last_brand": "TechCorp"})
    
    # Retrieve and verify
    context = context_manager.get_context(user_id, thread_id)
    assert context["user_context"]["preference"] == "detailed_analysis"
    assert context["thread_context"]["last_brand"] == "TechCorp"

def test_follow_up_suggestions(agents, context_manager):
    user_id = str(uuid.uuid4())
    thread_id = str(uuid.uuid4())
    context = context_manager.get_context(user_id, thread_id)
    
    question = "Tell me about the survey responses"
    response = agents.process_question(question, context)
    
    assert "vague" in response["question_type"].lower()
    # The main.py endpoint would generate follow-up suggestions for vague queries
