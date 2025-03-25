import pytest
import pandas as pd
from pathlib import Path
from src.data.generator import SurveyDataGenerator
from src.agents.agents import MarketResearchAgents
from src.utils.db_manager import DatabaseManager

@pytest.fixture
def survey_data():
    """Generate test survey data"""
    generator = SurveyDataGenerator()
    return generator.generate_dataset(num_users=10)  # Small dataset for testing

@pytest.fixture
def db_manager(survey_data, tmp_path):
    """Create a test database with sample data"""
    # Save test data to temporary parquet file
    test_data_path = tmp_path / "test_responses.parquet"
    survey_data.to_parquet(test_data_path)
    
    # Initialize DB manager with test data
    db_manager = DatabaseManager(data_dir=str(tmp_path))
    return db_manager

@pytest.fixture
def agents(db_manager):
    """Initialize agents with test database"""
    return MarketResearchAgents()

def test_data_generation(survey_data):
    """Test survey data generation"""
    # Check basic structure
    assert isinstance(survey_data, pd.DataFrame)
    assert len(survey_data) > 0
    
    # Check required columns exist
    required_columns = {
        'response_id', 'user_id', 'thread_id', 'brand_id', 'survey_id',
        'timestamp', 'question_id', 'question_type', 'question_text',
        'question_group', 'answer', 'age', 'gender', 'location',
        'income_bracket', 'education'
    }
    assert all(col in survey_data.columns for col in required_columns)
    
    # Check data types (all should be strings)
    assert all(survey_data[col].dtype == 'object' for col in survey_data.columns)
    
    # Check unique constraints
    assert survey_data['response_id'].nunique() == len(survey_data)  # Each response should be unique
    assert survey_data['thread_id'].nunique() <= 10  # We create 10 chat threads
    assert survey_data['user_id'].nunique() <= 10  # We created 10 users
    
    # Check question types
    valid_types = {'rating', 'open_ended', 'multiple_choice', 'scale'}
    assert all(qtype in valid_types for qtype in survey_data['question_type'].unique())

def test_db_manager_query_execution(db_manager):
    """Test database query execution"""
    # Test basic query
    query = "SELECT COUNT(*) as count FROM test_responses"
    result = db_manager.execute_query(query)
    assert isinstance(result, list)
    assert len(result) == 1
    assert 'count' in result[0]
    
    # Test complex query with type casting
    query = """
    SELECT 
        thread_id,
        COUNT(DISTINCT user_id) as num_respondents,
        AVG(CAST(answer AS INTEGER)) as avg_rating
    FROM test_responses
    WHERE question_type = 'rating'
    GROUP BY thread_id
    """
    result = db_manager.execute_query(query)
    assert isinstance(result, list)
    for row in result:
        assert 'thread_id' in row
        assert 'num_respondents' in row
        assert 'avg_rating' in row
        assert isinstance(row['num_respondents'], int)

def test_agent_question_processing(agents):
    """Test agent question processing"""
    # Test analytical question
    analytical_q = "What is the average rating for TechCorp's product quality?"
    result = agents.process_question(analytical_q, {})
    assert isinstance(result, dict)
    assert 'question_type' in result
    assert 'results' in result
    assert isinstance(result['results'], list)
    
    # Test insight question
    insight_q = "What are the common themes in customer feedback about product improvements?"
    result = agents.process_question(insight_q, {})
    assert isinstance(result, dict)
    assert 'question_type' in result
    assert 'results' in result
    
    # Test hybrid question
    hybrid_q = "How do product ratings compare across different age groups, and what insights can we draw from this?"
    result = agents.process_question(hybrid_q, {})
    assert isinstance(result, dict)
    assert 'question_type' in result
    assert 'results' in result
    assert len(result['results']) >= 2  # Should have both analytical and insight results

def test_demographic_analysis(db_manager):
    """Test demographic analysis queries"""
    query = """
    SELECT 
        CASE 
            WHEN CAST(age AS INTEGER) < 30 THEN 'Under 30'
            WHEN CAST(age AS INTEGER) < 50 THEN '30-50'
            ELSE 'Over 50'
        END as age_group,
        COUNT(DISTINCT user_id) as num_respondents,
        COUNT(*) as num_responses
    FROM test_responses
    GROUP BY age_group
    ORDER BY age_group
    """
    result = db_manager.execute_query(query)
    assert isinstance(result, list)
    assert len(result) > 0
    for row in result:
        assert 'age_group' in row
        assert 'num_respondents' in row
        assert 'num_responses' in row
        assert row['num_responses'] >= row['num_respondents']  # Each respondent has multiple answers

def test_thread_analysis(db_manager):
    """Test chat thread analysis"""
    query = """
    SELECT 
        thread_id,
        COUNT(DISTINCT user_id) as num_users,
        COUNT(DISTINCT brand_id) as num_brands,
        COUNT(*) as num_responses
    FROM test_responses
    GROUP BY thread_id
    """
    result = db_manager.execute_query(query)
    assert isinstance(result, list)
    assert len(result) > 0
    for row in result:
        assert row['num_users'] >= 1  # Each thread should have at least one user
        assert row['num_responses'] >= row['num_users']  # Each user should have at least one response
