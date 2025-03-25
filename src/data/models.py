from enum import Enum
from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field
from datetime import datetime

class QuestionType(str, Enum):
    OPEN_ENDED = "open_ended"
    MULTIPLE_CHOICE = "multiple_choice"
    RATING = "rating"
    GRID = "grid"
    SCALE = "scale"

class SurveyQuestion(BaseModel):
    question_id: str
    type: QuestionType
    text: str
    options: Optional[List[str]] = None
    scale_range: Optional[tuple[int, int]] = None
    grid_rows: Optional[List[str]] = None
    grid_columns: Optional[List[str]] = None

class SurveyResponse(BaseModel):
    response_id: str
    user_id: str
    thread_id: str
    brand_id: str
    survey_id: str
    timestamp: datetime
    demographics: Dict[str, str]  # age, gender, location, etc.
    answers: Dict[str, Union[str, int, List[str], Dict[str, Union[str, int]]]]

class ContextData(BaseModel):
    last_interaction: datetime
    previous_questions: List[str]
    user_preferences: Dict[str, str]
    current_focus: Optional[Dict[str, str]] = None
