import pandas as pd
from faker import Faker
from datetime import datetime
from typing import Dict
import uuid
from pathlib import Path

fake = Faker()

class SurveyDataGenerator:
    def __init__(self):
        self.brands = ["TechCorp", "EcoGoods", "HealthPlus"]
        self.surveys = {
            "TechCorp": {
                "S1": [
                    {
                        "question_id": "q1",
                        "question_type": "rating",
                        "question_text": "How would you rate the product quality?",
                        "question_group": "product",
                        "scale_min": 1,
                        "scale_max": 5
                    },
                    {
                        "question_id": "q2",
                        "question_type": "multiple_choice",
                        "question_text": "Which features do you use most?",
                        "question_group": "product",
                        "options": ["AI", "Cloud", "Security", "Mobile"]
                    }
                ]
            },
            "EcoGoods": {
                "S2": [
                    {
                        "question_id": "q1",
                        "question_type": "rating",
                        "question_text": "How eco-friendly is our packaging?",
                        "question_group": "sustainability",
                        "scale_min": 1,
                        "scale_max": 5
                    }
                ]
            },
            "HealthPlus": {
                "S3": [
                    {
                        "question_id": "q1",
                        "question_type": "multiple_choice",
                        "question_text": "Which health products do you use?",
                        "question_group": "products",
                        "options": ["Vitamins", "Supplements", "Protein", "Herbs"]
                    }
                ]
            }
        }
    
    def generate_demographics(self) -> Dict:
        """Generate demographic data for a user"""
        return {
            "age": str(fake.random_int(18, 80)),
            "gender": fake.random_element(["Male", "Female", "Other"]),
            "location": fake.city(),
            "income_bracket": fake.random_element(["Low", "Medium", "High"]),
            "education": fake.random_element(["High School", "Bachelor", "Master", "PhD"])
        }
    
    def generate_answer(self, question: Dict) -> str:
        """Generate an answer based on question type"""
        if question["question_type"] == "rating":
            return str(fake.random_int(question["scale_min"], question["scale_max"]))
        elif question["question_type"] == "multiple_choice":
            return fake.random_element(question["options"])
        elif question["question_type"] == "open_ended":
            return fake.text(max_nb_chars=200)
        elif question["question_type"] == "scale":
            return str(fake.random_int(question["scale_min"], question["scale_max"]))
        return ""

    def generate_dataset(self, num_users: int = 100) -> pd.DataFrame:
        """Generate a complete dataset of survey responses"""
        data = []
        
        # Generate survey responses
        for _ in range(num_users):
            survey_user_id = str(uuid.uuid4())
            demographics = self.generate_demographics()
            
            for brand in fake.random_elements(elements=self.brands, length=fake.random_int(1, len(self.brands))):
                for survey_name, questions in self.surveys[brand].items():
                    timestamp = fake.date_time_between(start_date="-30d", end_date="now")
                    
                    for question in questions:
                        response = {
                            "response_id": str(uuid.uuid4()),
                            "user_id": survey_user_id,
                            "brand_id": brand,
                            "survey_id": survey_name,
                            "timestamp": timestamp.isoformat(),
                            "question_id": question["question_id"],
                            "question_type": question["question_type"],
                            "question_text": question["question_text"],
                            "question_group": question["question_group"],
                            "answer": self.generate_answer(question)
                        }
                        
                        # Add scale information for rating/scale questions
                        if question["question_type"] in ["rating", "scale"]:
                            response["scale_min"] = str(question["scale_min"])
                            response["scale_max"] = str(question["scale_max"])
                        
                        # Add options for multiple choice questions
                        if question["question_type"] == "multiple_choice":
                            response["options"] = "|".join(question["options"])
                        
                        # Add demographics
                        response.update(demographics)
                        
                        data.append(response)
        
        # Create DataFrame with explicit dtypes for consistency
        df = pd.DataFrame(data)
        
        # Ensure all columns are string type for consistent parquet storage
        for col in df.columns:
            df[col] = df[col].astype(str)
        
        return df
    
    def save_to_parquet(self, output_dir: str = "data", num_users: int = 100):
        """Generate and save survey data to parquet file"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        df = self.generate_dataset(num_users)
        output_file = output_path / "survey_responses.parquet"
        df.to_parquet(output_file, index=False)
        print(f"Saved survey data to {output_file}")

if __name__ == "__main__":
    generator = SurveyDataGenerator()
    generator.save_to_parquet()
