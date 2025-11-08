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


# Node 3: Contextual Analyzer
def analyze_context(state: InvoiceAnalysisState) -> InvoiceAnalysisState:
    """
    Node 3: LLM contextual analysis using structured outputs
    
    This node uses LLM to understand WHY anomalies exist and if they're justified.
    Uses Pydantic model for structured output to ensure type safety.
    """
    logger.info("Starting contextual analysis with LLM")
    invoice = state["invoice_data"]
    historical = state["historical_data"]
    anomalies = state["anomalies"]
    
    # Prepare historical summary
    historical_summary = ""
    if historical and len(historical) > 0:
        avg_cost = sum(h["invoice_amount"] for h in historical) / len(historical)
        historical_summary = f"Average historical cost: €{avg_cost:.2f} (from {len(historical)} invoices)"
    else:
        historical_summary = "No historical data available"
    
    # Prepare sample historical data for context
    sample_historical = historical[:5] if historical else []
    
    # Create prompt
    prompt = f"""You are a freight cost analyst. Analyze this invoice and explain if the anomalies are justified.

CURRENT INVOICE:
{json.dumps(invoice, indent=2)}

HISTORICAL CONTEXT:
{historical_summary}
Sample historical invoices: {json.dumps(sample_historical, indent=2)}

DETECTED ANOMALIES:
{json.dumps(anomalies, indent=2)}

Analyze whether these anomalies are justified by considering:
- Weight differences compared to historical averages
- Seasonal patterns (current date: {invoice.get('shipment_date', 'unknown')})
- Service level requirements (express vs standard)
- Market conditions and fuel prices
- Route complexity

Provide your analysis with contextual factors, justification assessment, and confidence score."""
    
    try:
        # Use structured output for type-safe response
        llm_with_structure = llm.with_structured_output(ContextualAnalysis)
        
        result = llm_with_structure.invoke(prompt)
        
        # Extract structured data
        state["context_factors"] = result.contextual_factors
        state["reasoning"] = result.overall_assessment
        state["estimated_fair_cost"] = result.estimated_fair_cost
        state["confidence_score"] = result.confidence_in_analysis
        
        logger.info(f"Contextual analysis complete. Confidence: {result.confidence_in_analysis:.2f}")
        logger.info(f"Justified anomalies: {len(result.justified_anomalies)}, Suspicious: {len(result.suspicious_anomalies)}")
        
    except Exception as e:
        # Fallback if LLM call fails
        logger.error(f"LLM analysis failed: {e}")
        state["context_factors"] = ["Analysis error occurred - using fallback values"]
        state["reasoning"] = f"Error during analysis: {str(e)}. Using default values."
        state["estimated_fair_cost"] = state["expected_cost"]
        state["confidence_score"] = 0.5  # Default to medium confidence
        logger.warning("Using fallback values due to LLM error")
    
    return state


# Node 4: Recommendation Engine
def generate_recommendations(state: InvoiceAnalysisState) -> InvoiceAnalysisState:
    """
    Node 4: Final decision and recommendations
    
    This node determines the final status based on confidence score and
    generates actionable recommendations.
    """
    logger.info("Generating recommendations")
    confidence = state.get("confidence_score", 0.5)
    estimated_fair = state.get("estimated_fair_cost", state["expected_cost"])
    actual = state["invoice_data"]["invoice_amount"]
    anomalies = state.get("anomalies", [])
    
    recommendations = []
    
    # Determine status based on confidence
    if confidence >= 0.85:
        state["status"] = "approved"
        recommendations.append("Approve invoice - pricing within acceptable range")
        logger.info("Invoice approved automatically")
    elif confidence >= 0.40:
        state["status"] = "requires_review"
        recommendations.append("Manual review recommended - significant variance detected")
        
        if actual > estimated_fair * 1.10:
            savings = actual - estimated_fair
            recommendations.append(f"Request breakdown from carrier - potential savings: €{savings:.2f}")
        
        # Check for high-severity anomalies
        high_severity = [a for a in anomalies if a.get("severity") in ["high", "critical"]]
        if high_severity:
            recommendations.append("Verify contract terms - multiple high-severity anomalies detected")
        
        recommendations.append("Compare with 2 alternative carriers for benchmark pricing")
        logger.info("Invoice requires manual review")
    else:
        state["status"] = "rejected"
        recommendations.append("Reject invoice - significant pricing anomalies detected")
        recommendations.append("Escalate to procurement for carrier relationship review")
        logger.warning("Invoice rejected automatically")
    
    state["recommendations"] = recommendations
    logger.info(f"Generated {len(recommendations)} recommendations")
    
    return state


# Conditional routing function
def route_after_detection(state: InvoiceAnalysisState) -> str:
    """
    Route after anomaly detection: analyze if anomalies found, else approve
    
    This implements conditional routing based on workflow state.
    """
    anomalies = state.get("anomalies", [])
    if len(anomalies) > 0:
        logger.info(f"Routing to analyze context ({len(anomalies)} anomalies found)")
        return "analyze"
    else:
        # No anomalies - auto approve
        logger.info("No anomalies found - routing to direct approval")
        state["status"] = "approved"
        state["confidence_score"] = 0.95
        state["reasoning"] = "No significant anomalies detected"
        state["recommendations"] = ["Approve invoice - pricing is within acceptable range"]
        state["estimated_fair_cost"] = state["expected_cost"]
        state["context_factors"] = ["No anomalies detected - standard pricing"]
        return "recommend"


# Build the workflow
def build_analysis_graph():
    """
    Build and return the LangGraph workflow
    
    Architecture: Sequential Processing with Conditional Routing
    - Sequential: validate -> detect -> (conditional) -> analyze -> recommend
    - Conditional: Routes to analyze if anomalies found, else skips to recommend
    """
    logger.info("Building analysis graph")
    workflow = StateGraph(InvoiceAnalysisState)
    
    # Add nodes (single responsibility principle)
    workflow.add_node("validate", validate_business_logic)
    workflow.add_node("detect", detect_anomalies)
    workflow.add_node("analyze", analyze_context)
    workflow.add_node("recommend", generate_recommendations)
    
    # Set entry point
    workflow.add_edge(START, "validate")
    
    # Sequential edges
    workflow.add_edge("validate", "detect")
    
    # Conditional edge: route based on anomalies
    workflow.add_conditional_edges(
        "detect",
        route_after_detection,
        {
            "analyze": "analyze",
            "recommend": "recommend"
        }
    )
    
    # Final edges
    workflow.add_edge("analyze", "recommend")
    workflow.add_edge("recommend", END)
    
    logger.info("Graph built successfully")
    return workflow.compile()


# Compile the graph (required for LangGraph CLI)
graph = build_analysis_graph()