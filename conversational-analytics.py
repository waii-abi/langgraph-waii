import uuid
from io import StringIO
from typing import List, Optional

import pandas as pd
from pydantic import BaseModel
from langgraph.constants import START, END
from langgraph.graph import StateGraph
from langgraph.types import Send
from waii_sdk_py import WAII
from waii_sdk_py.query import QueryGenerationRequest, RunQueryRequest

class State(BaseModel):
    query: str = ''
    sql: str = ''
    data: List[str] = []
    chart: str = ''
    insight: str = ''
    response: str = ''
    error: Optional[str] = None
    attempts: int = 0
    path_decision: str = ""

def intent_classifier(state: State) -> State:
    print(f"Classifying and processing query: {state.query}")
    intent = waii_intent_classification(query=state.query)

    if intent == "insight":
        return state.model_copy(update={"path_decision": "insight"})
    elif intent == "sql":
        return state.model_copy(update={"path_decision": "sql"})
    elif intent == "data_visualization":
        return state.model_copy(update={"path_decision": "chart"})
    else:
        return state.model_copy(update={"error": "Unknown intent"})

def sql_generator(state: State) -> dict:
    print(f"Generating SQL for query: {state.query}")
    try:
        sql = waii_sql_generator(question=state.query)
        return {"sql": sql, "error": None}
    except Exception as e:
        return {"error": str(e)}

def sql_executor(state: State) -> dict:
    print(f"Executing query: {state.sql}")
    if state.error:
        return {}
    try:
        data = waii_sql_executor(query=state.sql)
        return {"data": data, "error": None}
    except Exception as e:
        return {"error": str(e)}

def chart_generator(state: State) -> dict:
    print(f"Generating chart for data: {state.data}")
    if state.error:
        return {}
    try:
        chart = waii_chart_generator(state.data)
        return {"chart": chart, "error": None}
    except Exception as e:
        return {"error": str(e)}

def insight_generator(state: State) -> dict:
    print(f"Generating insight for data: {state.data}")
    if state.error:
        return {}
    # TODO: Need to fix this for integration with WAII
    insight = waii_insight_generator(state.data)
    return {"insight": insight}

def result_synthesizer(state: State) -> dict:
    print(f"Formulating response with insight")
    if state.error:
        return {}
    # TODO: Need to fix this for integration with WAII
    return {"response": "Here is the formulated response", "error": None}

def decision_step(state: State) -> dict:
    print("Deciding the next step based on the query result...")
    # Example decision logic: If 'data' has more than one row, generate a chart.
    if len(state.data) > 1:
        print("Decision: Generating chart")
        return {"path_decision": "chart_generator"}
    else:
        print("Decision: Generating insight")
        return {"path_decision": "insight_generator"}

def decision_step_conditional_branch(state: State):
    print(f"Routing based on path_decision: {state.path_decision}")
    if state.path_decision == "chart_generator":
        return [Send("Chart Generator", state)]
    elif state.path_decision == "Insight Generator":
        return [Send("Insight Generator", state)]
    else:
        raise ValueError(f"Unknown path_decision: {state.path_decision}")

def should_retry_generic(state: State, success_next: str, retry_next: str = "SQL Generator"):
    print(f"\tChecking if we should retry in {success_next}: error: {state.error}")
    if state.error and state.attempts < 3:
        return [Send(retry_next, State(**state.dict(), attempts=state.attempts + 1, error=None))]
    print(f"Moving on to {success_next}")
    return [Send(success_next, State(**state.dict(), error=None, attempts=0))]

def create_retry_func(success_next: str):
    return lambda state: should_retry_generic(state, success_next)

def init_waii():
    url = 'http://localhost:9859/api/'
    api_key = ''
    db_connection_str = 'snowflake://WAII_USER@gqobxjv-bhb91428/MOVIE_DB?role=WAII_USER_ROLE&warehouse=COMPUTE_WH'
    WAII.initialize(url=url, api_key=api_key)
    WAII.Database.activate_connection(db_connection_str)
    print(f"Initialized WAII with connection: {db_connection_str}")

def waii_intent_classification(query: str) -> str:
    # TODO: Placeholder for intent classification
    return "sql"

def waii_sql_generator(question: str) -> str:
    try:
        query_id = str(uuid.uuid4())
        response = WAII.Query.generate(QueryGenerationRequest(uuid=query_id, ask=question))
        return response.query
    except Exception as e:
        print(f"Error generating query: {e}")
        return ""

def waii_sql_executor(query: str) -> List[str]:
    try:
        response = WAII.Query.run(RunQueryRequest(query=query))
        print(f"Executed the query, num of rows: {len(response.rows)}")
        return response.rows
    except Exception as e:
        print(f"Error executing query: {e}")
        return []

def waii_chart_generator(data: List[str]) -> str:
    try:
        # TODO: Need to convert data to df. This is dummy impl.
        df_data = pd.DataFrame(data)
        response = WAII.Chart.chart_generator(df=df_data)
        print(f"Chart spec: {response.chart_spec}")
        return response.chart_spec
    except Exception as e:
        print(f"Error generating chart: {e}")
        raise e

def waii_insight_generator(param: List[str]) -> str:
    # TODO: Need to integrate with WAII for generating query
    return "Insight: These are the top 5 directors."

# Initialize WAII
init_waii()

# create the wrokflow
workflow = StateGraph(State)

# Add nodes to the graph
workflow.add_node("Intent Classifier", intent_classifier)
workflow.add_node("SQL Generator", sql_generator)
workflow.add_node("SQL Executor", sql_executor)
workflow.add_node("Chart Generator", chart_generator)
workflow.add_node("Insight Generator", insight_generator)
workflow.add_node("Result Synthesizer", result_synthesizer)

# Define edges to control workflow execution
workflow.set_entry_point("Intent Classifier")
workflow.add_conditional_edges(
    "Intent Classifier",
    lambda state: state.path_decision,
    {
        "sql": "SQL Generator",
        "chart": "Chart Generator",
        "insight": "Insight Generator"
    }
)
workflow.add_edge("SQL Generator", "SQL Executor")
workflow.add_edge("SQL Executor", "Result Synthesizer")
workflow.add_edge("Chart Generator", "Result Synthesizer")
workflow.add_edge("Insight Generator", "Result Synthesizer")
workflow.set_finish_point("Result Synthesizer")

# init WAII related APIs.
init_waii()

# Compile the workflow
app = workflow.compile()

# Print the workflow graph
print(app.get_graph().draw_ascii())

def run_workflow(query: str) -> str:
    initial_state = State(query=query, attempts=0)
    final_state = app.invoke(initial_state)
    return final_state.response

# Example usage
if __name__ == "__main__":
    response = run_workflow("Who are the top 5 directors with the highest number of titles?")
    print(response)
