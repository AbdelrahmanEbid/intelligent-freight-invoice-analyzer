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


# Node 1: Business Logic Validator
def validate_business_logic(state: InvoiceAnalysisState) -> InvoiceAnalysisState:
    """
    Node 1: Validate business logic and detect basic anomalies
    
    This node performs rule-based validation:
    - Checks cost per km threshold
    - Checks cost per kg threshold
    - Validates service type appropriateness
    """
    logger.info("Starting business logic validation")
    invoice = state["invoice_data"]
    anomalies = state.get("anomalies", [])
    
    try:
        # Cost per km check
        cost_per_km = invoice["invoice_amount"] / invoice["distance_km"]
        if cost_per_km > 3.0:
            anomalies.append({
                "type": "high_cost_per_km",
                "severity": "medium",
                "description": f"Cost per km (€{cost_per_km:.2f}) exceeds typical range (€3.0/km)"
            })
            logger.warning(f"High cost per km detected: €{cost_per_km:.2f}")
        
        # Weight to cost ratio
        cost_per_kg = invoice["invoice_amount"] / invoice["weight_kg"]
        if cost_per_kg > 1.5:
            anomalies.append({
                "type": "high_cost_per_kg",
                "severity": "medium",
                "description": f"Cost per kg (€{cost_per_kg:.2f}) is unusually high (€1.5/kg)"
            })
            logger.warning(f"High cost per kg detected: €{cost_per_kg:.2f}")
        
        # Service type justification check
        service_type = invoice.get("service_type", "standard")
        weight = invoice.get("weight_kg", 0)
        if service_type == "express" and weight > 2000:
            anomalies.append({
                "type": "express_heavy_shipment",
                "severity": "low",
                "description": "Express service for heavy shipment (>2000kg) may not be optimal"
            })
            logger.info("Express service for heavy shipment detected")
        
        state["anomalies"] = anomalies
        logger.info(f"Validation complete. Found {len(anomalies)} initial anomalies")
        
    except KeyError as e:
        logger.error(f"Missing required field in invoice data: {e}")
        raise
    except ZeroDivisionError as e:
        logger.error(f"Division by zero error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in validation: {e}")
        raise
    
    return state


# Node 2: Anomaly Detector
def detect_anomalies(state: InvoiceAnalysisState) -> InvoiceAnalysisState:
    """
    Node 2: Statistical anomaly detection (simplified)
    
    This node compares invoice against:
    - Expected cost (from calculation)
    - Historical average (from historical data)
    """
    logger.info("Starting anomaly detection")
    invoice = state["invoice_data"]
    historical = state["historical_data"]
    expected = state["expected_cost"]
    anomalies = state.get("anomalies", [])
    
    try:
        actual = invoice["invoice_amount"]
        variance_percent = ((actual - expected) / expected) * 100
        
        # Price deviation from expected
        if abs(variance_percent) > 15:
            severity = "high" if abs(variance_percent) > 25 else "medium"
            anomalies.append({
                "type": "price_deviation",
                "severity": severity,
                "description": f"Invoice {variance_percent:+.1f}% from expected cost",
                "expected": expected,
                "actual": actual,
                "variance": variance_percent
            })
            logger.warning(f"Price deviation detected: {variance_percent:+.1f}%")
        
        # Historical average comparison
        if historical and len(historical) > 0:
            costs = [h["invoice_amount"] for h in historical]
            avg_cost = sum(costs) / len(costs)
            avg_variance = ((actual - avg_cost) / avg_cost) * 100
            
            if abs(avg_variance) > 20:
                anomalies.append({
                    "type": "historical_outlier",
                    "severity": "medium",
                    "description": f"Cost is {avg_variance:+.1f}% different from historical average (€{avg_cost:.2f})"
                })
                logger.warning(f"Historical outlier detected: {avg_variance:+.1f}% from average")
        
        state["anomalies"] = anomalies
        logger.info(f"Anomaly detection complete. Total anomalies: {len(anomalies)}")
        
    except KeyError as e:
        logger.error(f"Missing required field: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in anomaly detection: {e}")
        raise
    
    return state

