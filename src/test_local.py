from agents.agents import MarketResearchAgents
import json
from typing import Dict, List

def get_test_questions() -> Dict[str, List[str]]:
    """Get test questions by category"""
    return {
        "analytical": [
            # "What percentage of respondents are female?",
            # "What is the average rating for product quality?",
            # "How many users are in the 30-50 age group?",
            "What's the distribution of education levels among respondents?", # came out wrong in comparision
            "What percentage gave a rating of 4 or higher for product quality?" # didn't work gave NAN as answer
        ],
        "insight": [
            "What are the most common features users prefer?",
            "What patterns do you see in product satisfaction across age groups?",
            "What recommendations would you make to improve user satisfaction?",
            "What insights can you draw from the education level distribution?",
            "How do different demographics perceive our product features?"
        ],
        "hybrid": [
            "Analyze product quality ratings across different age groups and suggest improvements",
            "What features are most popular among highly satisfied customers?",
            "Compare satisfaction levels between education groups and suggest targeted improvements",
            "What demographic groups show the lowest satisfaction and why?",
            "Analyze the correlation between preferred features and satisfaction ratings"
        ]
    }

def main():
    # Initialize agents
    agents = MarketResearchAgents()
    
    # Get test questions
    questions = get_test_questions()
    
    # Test context
    context = {
        "brand_id": "TechCorp",
        "survey_id": "S1"
    }
    
    # Process each question type
    for question_type, test_questions in questions.items():
        print(f"\n{'='*80}")
        print(f"Testing {question_type.upper()} questions:")
        print('='*80)
        
        for i, question in enumerate(test_questions, 1):
            print(f"\n{i}. Question: {question}")
            print('-'*40)
            
            # Get user input to continue
            input("Press Enter to process this question...")
            
            response = agents.process_question(
                question=question,
                context=context,
                thread_id="test",
                user_id="test_user"
            )
            
            # Pretty print response
            print("\nResponse:")
            print(json.dumps(response, indent=2))
            print("\n" + "-"*80)

if __name__ == "__main__":
    main()
