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
    justified_anomalies: List[str]  # Anomaly types that are justified
    suspicious_anomalies: List[str]  # Anomaly types that remain concerning
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
    
    # Start with anomalies from validation node (business logic checks)
    # But filter out any anomalies from previous test cases to prevent state contamination
    existing_anomalies = state.get("anomalies", [])
    current_amount = invoice["invoice_amount"]
    current_invoice_id = invoice.get("invoice_id", "")
    
    # Filter anomalies:
    # 1. Keep validation anomalies (they don't have "actual" field - cost_per_km, cost_per_kg checks)
    # 2. Keep anomalies that match current invoice amount
    # 3. Remove anomalies from previous invoices (state contamination)
    anomalies = []
    for a in existing_anomalies:
        anomaly_actual = a.get("actual")
        # Validation anomalies don't have "actual" field - keep them
        if anomaly_actual is None:
            anomalies.append(a)
        # Detection anomalies with "actual" field - only keep if matches current invoice
        elif anomaly_actual == current_amount:
            anomalies.append(a)
        # Otherwise, it's from a previous invoice - skip it
        else:
            logger.warning(f"Filtering out anomaly from previous invoice: {a.get('description', 'unknown')} (amount: {anomaly_actual})")
    
    logger.info(f"Starting anomaly detection with {len(anomalies)} existing anomalies (after filtering, invoice: {current_invoice_id})")
    
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
    anomalies = state.get("anomalies", [])
    
    # Safety check: If no anomalies, skip LLM and approve directly
    if len(anomalies) == 0:
        logger.info("No anomalies detected - skipping LLM analysis and approving")
        state["context_factors"] = ["No anomalies detected - standard pricing"]
        state["reasoning"] = "No significant anomalies detected. Invoice pricing is within acceptable range."
        state["estimated_fair_cost"] = state["expected_cost"]
        state["confidence_score"] = 0.95
        state["justified_anomalies"] = []
        state["suspicious_anomalies"] = []
        return state
    
    # Prepare historical summary
    historical_summary = ""
    if historical and len(historical) > 0:
        avg_cost = sum(h["invoice_amount"] for h in historical) / len(historical)
        historical_summary = f"Average historical cost: €{avg_cost:.2f} (from {len(historical)} invoices)"
    else:
        historical_summary = "No historical data available"
    
    # Prepare sample historical data for context
    sample_historical = historical[:5] if historical else []
    
    # Calculate variance for context
    actual = invoice["invoice_amount"]
    expected = state["expected_cost"]
    variance_percent = ((actual - expected) / expected) * 100 if expected > 0 else 0
    
    # Create prompt with better guidance
    prompt = f"""You are an experienced freight cost analyst. Analyze this invoice and determine if the price variance is justified.

CURRENT INVOICE:
{json.dumps(invoice, indent=2)}

EXPECTED COST: €{expected:.2f}
ACTUAL COST: €{actual:.2f}
VARIANCE: {variance_percent:+.2f}%

HISTORICAL CONTEXT:
{historical_summary}
Sample historical invoices: {json.dumps(sample_historical, indent=2)}

DETECTED ANOMALIES:
{json.dumps(anomalies, indent=2)}

ANALYSIS INSTRUCTIONS:
1. Consider ALL contextual factors that could justify the variance:
   - Service level differences (express vs standard typically adds 30-50%)
   - Seasonal demand (pre-holiday periods often add 10-20% premium)
   - Fuel price increases (can add 15-25% in volatile markets)
   - Market capacity constraints (can add 10-15% during peak times)
   - Weight and route differences
   - Carrier-specific rate adjustments

2. CONFIDENCE SCORING (CRITICAL - Read carefully):
   - HIGH confidence (0.85-1.0): You are CONFIDENT the invoice is JUSTIFIED and should be APPROVED
   - MEDIUM confidence (0.40-0.85): You are UNSURE - some factors justify it, some don't → REQUIRES REVIEW
   - LOW confidence (0.0-0.40): You are CONFIDENT the invoice is UNJUSTIFIED and should be REJECTED
   
   IMPORTANT: 
   - If you say "unjustified", "fraud", "billing error" → confidence MUST be LOW (< 0.40)
   - If you say "justified", "acceptable", "reasonable" → confidence SHOULD be HIGH (≥ 0.70)
   - If ALL anomalies are suspicious and NONE justified → confidence MUST be LOW (< 0.40)
   - If variance > 100% with no strong justification → confidence MUST be LOW (< 0.40)
   
3. Be REASONABLE in your assessment:
   - If variance is 15-30% and contextual factors exist → confidence 0.50-0.75 (REVIEW)
   - If variance is 30-50% with strong justification (express service) → confidence 0.70-0.90 (APPROVE or REVIEW)
   - If variance is 30-50% without strong justification → confidence 0.40-0.60 (REVIEW)
   - If variance > 100% with no strong justification → confidence 0.15-0.30 (REJECT)
   - Only use confidence < 0.40 (REJECT) for cases where invoice is clearly unjustified

4. For THIS specific case:
   - Variance: {variance_percent:.1f}%
   - Service type: {invoice.get('service_type', 'standard')}
   - Consider if this variance could be explained by legitimate factors
   - Be balanced: don't be too strict, but don't approve everything

5. Provide DETAILED reasoning (2-3 sentences minimum) explaining your assessment.

Return your analysis with:
- contextual_factors: List all factors that could affect cost (even if uncertain)
- justified_anomalies: List which anomalies/types are justified and why
- suspicious_anomalies: List which anomalies/types remain concerning and why
- overall_assessment: Detailed explanation (2-3 sentences) of your reasoning
- estimated_fair_cost: What you think the fair cost should be (considering all factors)
- confidence_in_analysis: Your confidence (0.0-1.0) - be reasonable based on variance level and justification"""
    
    try:
        # Use structured output for type-safe response
        llm_with_structure = llm.with_structured_output(ContextualAnalysis)
        
        result = llm_with_structure.invoke(prompt)
        
        # Extract structured data
        state["context_factors"] = result.contextual_factors
        state["reasoning"] = result.overall_assessment
        state["estimated_fair_cost"] = result.estimated_fair_cost
        state["confidence_score"] = result.confidence_in_analysis
        state["justified_anomalies"] = result.justified_anomalies
        state["suspicious_anomalies"] = result.suspicious_anomalies
        
        logger.info(f"Contextual analysis complete. Confidence: {result.confidence_in_analysis:.2f}")
        logger.info(f"Justified anomalies: {len(result.justified_anomalies)}, Suspicious: {len(result.suspicious_anomalies)}")
        logger.info(f"Justified: {result.justified_anomalies}")
        logger.info(f"Suspicious: {result.suspicious_anomalies}")
        
        # Safety: Ensure confidence is reasonable based on variance, service type, and reasoning
        invoice_service = invoice.get("service_type", "standard")
        reasoning_lower = state.get("reasoning", "").lower()
        justified_count = len(state.get("justified_anomalies", []))
        suspicious_count = len(state.get("suspicious_anomalies", []))
        
        # CRITICAL: Check for contradictions between reasoning and confidence
        # If reasoning says "unjustified", "fraud", "error", "billing error" → confidence should be LOW
        rejection_keywords = ["unjustified", "fraud", "billing error", "error", "cannot be justified", 
                             "likely reflects", "appears to be", "warrants rejection", "should be rejected"]
        if any(keyword in reasoning_lower for keyword in rejection_keywords):
            if state["confidence_score"] > 0.40:
                logger.error(f"CRITICAL: Reasoning indicates rejection but confidence is {state['confidence_score']:.2f} - adjusting to 0.25")
                state["confidence_score"] = 0.25
        
        # CRITICAL: If ALL anomalies are suspicious and NONE justified, confidence should be LOW
        if suspicious_count > 0 and justified_count == 0 and state["confidence_score"] > 0.40:
            logger.error(f"CRITICAL: All anomalies suspicious but confidence is {state['confidence_score']:.2f} - adjusting to 0.30")
            state["confidence_score"] = min(state["confidence_score"], 0.30)
        
        # CRITICAL: Extreme variance (>100%) should never have high confidence
        if abs(variance_percent) > 100 and state["confidence_score"] > 0.40:
            logger.error(f"CRITICAL: Extreme variance ({variance_percent:.2f}%) but confidence is {state['confidence_score']:.2f} - adjusting to 0.20")
            state["confidence_score"] = 0.20
        
        # Adjust confidence if LLM is too strict or too lenient (but only if no contradictions)
        if abs(variance_percent) < 1 and state["confidence_score"] < 0.85:
            logger.warning(f"Very small variance ({variance_percent:.2f}%) but low confidence ({state['confidence_score']:.2f}) - adjusting to 0.90")
            state["confidence_score"] = 0.90
        elif abs(variance_percent) < 5 and state["confidence_score"] < 0.70:
            logger.warning(f"Small variance ({variance_percent:.2f}%) but low confidence ({state['confidence_score']:.2f}) - adjusting to 0.75")
            state["confidence_score"] = 0.75
        elif invoice_service == "express" and abs(variance_percent) < 60 and state["confidence_score"] < 0.50:
            # Express service with moderate variance should not be rejected
            logger.warning(f"Express service with {variance_percent:.2f}% variance but low confidence ({state['confidence_score']:.2f}) - adjusting to 0.60")
            state["confidence_score"] = 0.60
        elif abs(variance_percent) >= 15 and abs(variance_percent) <= 30 and state["confidence_score"] < 0.40:
            # Moderate variance (15-30%) should at least be reviewed, not rejected
            logger.warning(f"Moderate variance ({variance_percent:.2f}%) but very low confidence ({state['confidence_score']:.2f}) - adjusting to 0.45 for review")
            state["confidence_score"] = 0.45
        elif abs(variance_percent) > 100 and state["confidence_score"] < 0.20:
            # Extreme variance should have very low confidence
            logger.warning(f"Extreme variance ({variance_percent:.2f}%) - ensuring low confidence ({state['confidence_score']:.2f})")
            state["confidence_score"] = min(state["confidence_score"], 0.20)
        
        # Ensure reasoning is not empty or too brief
        if not state.get("reasoning") or len(state.get("reasoning", "")) < 20:
            logger.warning("Reasoning is too brief or empty - generating fallback reasoning")
            if len(state.get("justified_anomalies", [])) > 0:
                state["reasoning"] = f"Variance of {variance_percent:.1f}% is partially justified by contextual factors. Some anomalies are explained, but others require clarification. Manual review recommended."
            else:
                state["reasoning"] = f"Variance of {variance_percent:.1f}% is significant and requires investigation. No clear justification found for the price difference. Manual review required."
        
        # Ensure context factors are not empty
        if not state.get("context_factors") or len(state.get("context_factors", [])) == 0:
            logger.warning("Context factors are empty - generating fallback factors")
            fallback_factors = []
            if invoice_service == "express":
                fallback_factors.append("Express service typically incurs premium over standard service")
            if abs(variance_percent) > 20:
                fallback_factors.append(f"Significant variance ({variance_percent:.1f}%) requires investigation")
            fallback_factors.append("Seasonal and market factors may contribute to cost differences")
            state["context_factors"] = fallback_factors
        
    except Exception as e:
        # Fallback if LLM call fails
        logger.error(f"LLM analysis failed: {e}")
        import traceback
        logger.error(f"Error details: {traceback.format_exc()}")
        
        # Use variance and service type to determine fallback confidence
        invoice_service = invoice.get("service_type", "standard")
        
        if abs(variance_percent) < 5:
            fallback_confidence = 0.85
            fallback_reasoning = "LLM analysis unavailable, but variance is small - approving based on variance alone"
        elif invoice_service == "express" and abs(variance_percent) < 60:
            fallback_confidence = 0.65
            fallback_reasoning = f"LLM analysis unavailable. Express service with {variance_percent:.1f}% variance - review recommended"
        elif abs(variance_percent) < 30:
            fallback_confidence = 0.55
            fallback_reasoning = f"LLM analysis unavailable. Moderate variance ({variance_percent:.1f}%) - review recommended"
        elif abs(variance_percent) < 100:
            fallback_confidence = 0.35
            fallback_reasoning = f"LLM analysis unavailable. High variance ({variance_percent:.1f}%) - review required"
        else:
            fallback_confidence = 0.15
            fallback_reasoning = f"LLM analysis unavailable. Extreme variance ({variance_percent:.1f}%) - rejection recommended"
        
        state["context_factors"] = [
            "LLM analysis unavailable - using fallback values",
            f"Variance: {variance_percent:.1f}%",
            "Service type: " + invoice_service
        ]
        state["reasoning"] = fallback_reasoning
        state["estimated_fair_cost"] = state["expected_cost"]
        state["confidence_score"] = fallback_confidence
        state["justified_anomalies"] = []
        state["suspicious_anomalies"] = [a.get("type", "unknown") for a in anomalies] if anomalies else []
        logger.warning(f"Using fallback values due to LLM error. Set confidence to {fallback_confidence}")
    
    return state


# Node 4: Recommendation Engine
def generate_recommendations(state: InvoiceAnalysisState) -> InvoiceAnalysisState:
    """
    Node 4: Final decision and recommendations
    
    This node determines the final status based on confidence score and
    generates actionable recommendations.
    """
    logger.info("Generating recommendations")

    confidence = state.get("confidence_score", 0.0) # Default to 0.0 to make the safety check work
    estimated_fair = state.get("estimated_fair_cost", state["expected_cost"])
    actual = state["invoice_data"]["invoice_amount"]
    anomalies = state.get("anomalies", [])
    
    logger.info(f"Current confidence score: {confidence}")
    logger.info(f"Number of anomalies: {len(anomalies)}")
    
    recommendations = []
    
    # Safety check: If no anomalies and confidence is 0, this means routing didn't work correctly
    # Set high confidence for approval
    if len(anomalies) == 0 and confidence == 0.0:
        logger.warning("No anomalies detected but confidence is 0 - setting to approved")
        confidence = 0.95
        state["confidence_score"] = 0.95
        state["reasoning"] = "No significant anomalies detected - invoice approved"
        state["estimated_fair_cost"] = state["expected_cost"]
    
    # Determine status based on confidence
    if confidence >= 0.85:
        state["status"] = "approved"
        recommendations.append("Approve invoice - pricing within acceptable range")
        logger.info(f"Invoice approved automatically (confidence: {confidence:.2f})")
    elif confidence >= 0.40:
        state["status"] = "requires_review"
        
        # Get justification information from LLM analysis
        justified_anomalies = state.get("justified_anomalies", [])
        suspicious_anomalies = state.get("suspicious_anomalies", [])
        all_anomaly_types = [a.get("type", "unknown") for a in anomalies]
        
        # Check if service type difference explains the variance
        invoice_service = state["invoice_data"].get("service_type", "standard")
        has_service_type_justification = any(
            "express" in str(factor).lower() or "service" in str(factor).lower() 
            for factor in state.get("context_factors", [])
        )
        
        # Determine recommendation tone based on justification
        if len(justified_anomalies) > 0 and len(suspicious_anomalies) == 0:
            # All anomalies are justified - more positive recommendations
            recommendations.append("Manual review recommended - variance appears justified by service level or market conditions")
            
            if invoice_service == "express":
                recommendations.append("Express service typically incurs 30-70% premium - verify this matches contract terms")
            elif has_service_type_justification:
                recommendations.append("Service level difference explains variance - verify service type matches order requirements")
            
            # Only suggest savings if variance is still high after justification
            if actual > estimated_fair * 1.20:  # Higher threshold for justified cases
                savings = actual - estimated_fair
                recommendations.append(f"Request detailed breakdown to confirm all charges are justified - potential savings: €{savings:.2f}")
            else:
                recommendations.append("Review recommended for documentation, but pricing appears reasonable for service level")
                
        elif len(justified_anomalies) > 0 and len(suspicious_anomalies) > 0:
            # Mixed: some justified, some suspicious - provide nuanced recommendations
            recommendations.append("Manual review recommended - variance partially justified, but some concerns remain")
            
            # Acknowledge what's justified first
            if invoice_service == "express":
                recommendations.append("✓ Express service premium is justified (typically 30-50% over standard)")
            elif has_service_type_justification:
                recommendations.append("✓ Service level difference explains part of the variance")
            
            # Check context factors for other justifications
            context_factors = state.get("context_factors", [])
            if any("seasonal" in str(f).lower() for f in context_factors):
                recommendations.append("✓ Seasonal factors may contribute to higher costs")
            if any("fuel" in str(f).lower() for f in context_factors):
                recommendations.append("✓ Fuel surcharges may be justified based on current market rates")
            
            # Now address what's suspicious
            variance_percent = ((actual - estimated_fair) / estimated_fair) * 100 if estimated_fair > 0 else 0
            
            # Calculate estimated justified portion
            if invoice_service == "express":
                # Express typically adds 30-50%, so estimate justified cost
                estimated_justified_cost = estimated_fair * 1.40  # Use 40% as middle ground
                potentially_unjustified = max(0, actual - estimated_justified_cost)
                
                if potentially_unjustified > 50:  # More than €50 potentially unjustified
                    recommendations.append(f"⚠ The invoice exceeds typical express premium ({variance_percent:.1f}% vs expected 30-50%)")
                    recommendations.append(f"⚠ Focus review on the excess amount (~€{potentially_unjustified:.2f} above typical express rate)")
                    recommendations.append(f"Request itemized breakdown to verify: express surcharge %, fuel surcharge, and any additional fees")
                else:
                    recommendations.append("Request itemized breakdown to confirm all charges match contract terms")
            else:
                # For other cases, use standard approach
                if actual > estimated_fair * 1.10:
                    savings = actual - estimated_fair
                    recommendations.append(f"Request breakdown from carrier - focus on charges exceeding justified factors (potential savings: €{savings:.2f})")
            
            # Check for high-severity suspicious anomalies
            # Note: suspicious_anomalies contains descriptions, not types, so we check differently
            high_severity = [a for a in anomalies if a.get("severity") in ["high", "critical"]]
            if high_severity:
                # Only mention express premium if it's actually an express service
                if invoice_service == "express":
                    recommendations.append("⚠ Verify contract terms for high-severity anomalies - ensure express premium rates match agreement")
                else:
                    recommendations.append("⚠ Verify contract terms for high-severity anomalies - ensure all charges match contract agreement")
            
            # Add specific action based on suspicious factors
            if any("exceeds typical" in str(s).lower() or "cannot establish" in str(s).lower() for s in suspicious_anomalies):
                # Only mention express service benchmark if it's express
                if invoice_service == "express":
                    recommendations.append("Compare with 2-3 alternative carriers for express service on same route to establish benchmark")
                else:
                    recommendations.append("Compare with 2-3 alternative carriers for standard service on same route to establish benchmark")
            
            # Final recommendation
            recommendations.append("Approve justified portion, request clarification on excess charges before full payment")
            
        else:
            # No justification or all suspicious - standard cautious recommendations
            recommendations.append("Manual review recommended - significant variance detected")
            
            if actual > estimated_fair * 1.10:
                savings = actual - estimated_fair
                recommendations.append(f"Request breakdown from carrier - potential savings: €{savings:.2f}")
            
            # Check for high-severity anomalies
            high_severity = [a for a in anomalies if a.get("severity") in ["high", "critical"]]
            if high_severity:
                recommendations.append("Verify contract terms - multiple high-severity anomalies detected")
            
            recommendations.append("Compare with 2 alternative carriers for benchmark pricing")
        
        logger.info(f"Invoice requires manual review (confidence: {confidence:.2f}, justified: {len(justified_anomalies)}, suspicious: {len(suspicious_anomalies)})")
    else:
        # Only reject if there are actual anomalies or very low confidence with high variance
        variance_percent = ((actual - estimated_fair) / estimated_fair) * 100 if estimated_fair > 0 else 0
        
        # Safety: Don't reject if variance is small (< 10%) even with low confidence
        if abs(variance_percent) < 10 and len(anomalies) == 0:
            logger.warning(f"Low confidence ({confidence:.2f}) but small variance ({variance_percent:.2f}%) - approving instead of rejecting")
            state["status"] = "approved"
            state["confidence_score"] = 0.85
            recommendations.append("Approve invoice - pricing within acceptable range despite low confidence")
        else:
            state["status"] = "rejected"
            recommendations.append("Reject invoice - significant pricing anomalies detected")
            recommendations.append("Escalate to procurement for carrier relationship review")
            logger.warning(f"Invoice rejected automatically (confidence: {confidence:.2f}, variance: {variance_percent:.2f}%)")
    
    # Ensure reasoning is set
    if not state.get("reasoning") or state.get("reasoning") == "":
        if state["status"] == "approved":
            state["reasoning"] = "No significant anomalies detected - invoice approved"
        else:
            state["reasoning"] = "Analysis completed with recommendations"
    
    state["recommendations"] = recommendations
    logger.info(f"Generated {len(recommendations)} recommendations. Final status: {state['status']}")
    
    return state


# Conditional routing function
def route_after_detection(state: InvoiceAnalysisState) -> str:
    """
    Route after anomaly detection: analyze if anomalies found, else approve
    
    This implements conditional routing based on workflow state.
    """
    anomalies = state.get("anomalies", [])
    logger.info(f"Routing decision: {len(anomalies)} anomalies detected")
    
    if len(anomalies) > 0:
        logger.info(f"Routing to analyze context ({len(anomalies)} anomalies found)")
        return "analyze"
    else:
        # No anomalies - set approval values and route to recommend
        # Note: We set values here but recommend node will also set them as safety
        logger.info("No anomalies found - routing to direct approval")
        state["status"] = "approved"
        state["confidence_score"] = 0.95
        state["reasoning"] = "No significant anomalies detected - invoice pricing is within acceptable range"
        state["recommendations"] = ["Approve invoice - pricing is within acceptable range"]
        state["estimated_fair_cost"] = state.get("expected_cost", state["invoice_data"]["invoice_amount"])
        state["context_factors"] = ["No anomalies detected - standard pricing"]
        state["justified_anomalies"] = []
        state["suspicious_anomalies"] = []
        logger.info("Set approval values in routing function")
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