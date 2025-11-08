"""
Intelligent Freight Invoice Analyzer
LangGraph Agent for Invoice Analysis

Architecture Pattern: Sequential Processing with Conditional Routing
- Sequential pipeline: validate -> detect -> (conditional) -> analyze -> recommend
- Conditional routing based on anomaly detection results
- State-based workflow for data flow between nodes

Best Practices Applied:
1. Structured outputs using Pydantic models for LLM responses
2. Type-safe state management with TypedDict
3. Error handling with fallback values
4. Separation of concerns (each node has single responsibility)
5. Logging for debugging and monitoring
"""
from typing_extensions import TypedDict, Literal
from typing import List, Optional
from langchain.chat_models import init_chat_model
from langgraph.graph import StateGraph, START, END
from pydantic import BaseModel, Field
import json
import logging
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize LLM
llm = init_chat_model(
    "openai:gpt-oss-20b:free",
    temperature=0.3,
)

# Pydantic models for structured LLM outputs
class ContextualAnalysis(BaseModel):
    """Structured output for contextual analysis"""
    contextual_factors: List[str] = Field(description="List of contextual factors affecting cost")
    justified_anomalies: List[str] = Field(description="Anomaly types that are justified")
    suspicious_anomalies: List[str] = Field(description="Anomaly types that remain concerning")
    overall_assessment: str = Field(description="Detailed explanation in 2-3 sentences")
    estimated_fair_cost: float = Field(description="Estimated fair cost in EUR")
    confidence_in_analysis: float = Field(ge=0.0, le=1.0, description="Confidence score 0.0-1.0")


# State Definition (Type-safe with TypedDict)
class InvoiceAnalysisState(TypedDict):
    """State for invoice analysis workflow"""
    invoice_data: dict
    historical_data: List[dict]
    expected_cost: float
    anomalies: List[dict]
    context_factors: List[str]
    confidence_score: float
    status: str
    reasoning: str
    recommendations: List[str]
    estimated_fair_cost: float

