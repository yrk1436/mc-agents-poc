import duckdb
from utils.db_manager import DatabaseManager

def get_verification_queries():
    """Get verification queries for analytical questions"""
    b_filter = "brand_id = 'TechCorp' and survey_id = 'S1'"
    return [
        {
            "question": "What percentage of respondents are female?",
            "queries": [
                f"""
                WITH stats AS (
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN gender = 'Female' THEN 1 END) as female_count
                    FROM 'data/survey_responses.parquet'
                    WHERE {b_filter}
                )
                SELECT 
                    female_count,
                    total,
                    ROUND(100.0 * female_count / total, 2) as female_percentage
                FROM stats
                """
            ]
        },
        {
            "question": "What is the average rating for product quality?",
            "queries": [
                f"""
                SELECT 
                    COUNT(*) as num_responses,
                    ROUND(AVG(CAST(answer AS FLOAT)), 2) as avg_rating
                FROM 'data/survey_responses.parquet'
                WHERE {b_filter}
                AND question_id = 'q1'
                AND question_type = 'rating'
                """
            ]
        },
        {
            "question": "How many users are in the 30-50 age group?",
            "queries": [
                f"""
                SELECT 
                    COUNT(DISTINCT user_id) as users_30_50
                FROM 'data/survey_responses.parquet'
                WHERE {b_filter}
                AND CAST(age AS INTEGER) BETWEEN 30 AND 50
                """
            ]
        },
        {
            "question": "What's the distribution of education levels among respondents?",
            "queries": [
                f"""
                SELECT 
                    education,
                    COUNT(DISTINCT user_id) as count,
                    ROUND(100.0 * COUNT(DISTINCT user_id) / SUM(COUNT(DISTINCT user_id)) OVER (), 2) as percentage
                FROM 'data/survey_responses.parquet'
                WHERE {b_filter}
                GROUP BY education
                ORDER BY count DESC
                """
            ]
        },
        {
            "question": "What percentage gave a rating of 4 or higher for product quality?",
            "queries": [
                f"""
                WITH stats AS (
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN CAST(answer AS INTEGER) >= 4 THEN 1 END) as high_ratings
                    FROM 'data/survey_responses.parquet'
                    WHERE {b_filter}
                    AND question_id = 'q1'
                    AND question_type = 'rating'
                )
                SELECT 
                    high_ratings,
                    total,
                    ROUND(100.0 * high_ratings / total, 2) as high_rating_percentage
                FROM stats
                """
            ]
        }
    ]

def main():
    # Method 1: Direct DuckDB
    con = duckdb.connect()
    
    # Method 2: Using DatabaseManager
    db = DatabaseManager()
    
    # Get verification queries
    queries = get_verification_queries()
    
    for test_case in queries:
        print("\n" + "="*80)
        print(f"Question: {test_case['question']}")
        print("="*80)
        
        print("\nMethod 1 - Direct DuckDB:")
        for query in test_case['queries']:
            result = con.execute(query).fetchdf()
            print(result.to_string())
        
        print("\nMethod 2 - DatabaseManager:")
        for query in test_case['queries']:
            result = db.execute_query(query)
            for row in result:
                for k, v in row.items():
                    print(f"{k}: {v}")
        
        # Wait for user input
        input("\nPress Enter for next question...")

if __name__ == "__main__":
    main()